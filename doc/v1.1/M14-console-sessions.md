---
updated: 2026-11-25
domain: Auth
tags: [v1.1, implementation-plan, m14, console, sign-in, sessions, oauth2, pkce, csrf, api]
---

# M14 — The Console's Sign-in and Sessions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The 1.1.0 admin console's sign-in (Admin_Console §9, §14): how a person signs in under
each `globals.api.auth` mode, the session that keeps them signed in on every node, and the pages
that say when they aren't (401) or may not (403). Also M13f's two carry-ins: a stream's peers
verify a session as they verify a token, and a stream ends with its caller's credentials.

**Architecture:**

- **Sessions every node opens** (Task 1, `data-avalanche-api`): the session cookie, sealed with a
  key derived from `globals.console.session_key` (`session.rs`). `Auth` opens it when a request has
  no `Authorization` and verifies its person again, as it verifies credentials. The API takes the
  session, and a request it authenticates that changes something carries the session's
  anti-forgery token. A node passes a caller's session on to its peers with its other
  credentials.
- **A stream ends with its credentials** (Task 2): an open stream checks its caller every minute
  and at the credentials' expiry.
- **The console's routes under `/ui/`** (Task 3, `console.rs`, `pages.rs`): the `basic` sign-in
  form, its failures counted per address; the sign-out; `GET /ui/session`; the 401 and 403 pages
  (and `jwt`'s "No valid token", `none`'s banner), rendered by the server under a strict
  Content-Security-Policy; served while the console is enabled.
- **The `oauth2` sign-in** (Task 4, `oauth2.rs`): authorization code with PKCE against the
  provider, the code exchanged by the server, the session refreshed with the provider's refresh
  token, the sign-out taken on to the provider.
- **The site and the research docs** (Task 5), and **the E2E scenario** (Task 6): on three
  nodes, a session started on one node works on every node, for pages, the API, a forwarded
  node's detail and a stream that follows its peers with it, under `basic` and `oauth2`.

**Tech Stack:** Rust 1.100.0 (edition 2024). No new crate: `aws-lc-rs` (AES-256-GCM, SHA-256, the
random source), already built for rustls and jsonwebtoken, becomes a direct dependency of
`data-avalanche-api`. axum 0.8's `ConnectInfo` gives the client's address.

**Spec:**
- `doc/research/Admin_Console.md` §3 (the switch), §9 (sign-in and sessions), §10 (served under
  `/ui/`, forwarding with the caller's own credentials), §8.1 (the stream, M13f's notes for M14),
  §14. Task 5 adds §9.1, the sessions as built.
- The approved mockup `doc/v1.1/mockups/02-sign-in.html` (the sign-in per mode, the error pages,
  `none`'s banner).
- `doc/specs/Configuration_Specification.md` §6.3.1 (`oauth2.client_id`, `client_secret`,
  `scopes`, `idp_name`), §6.8 (`globals.console`), V-128…V-136. Amended by S-M14.1, below.
- `doc/specs/System-Overview.md` §Web console. Amended by S-M14.1, below.
- `doc/research/API_RBAC_and_Versioning.md` §3 (role binding per mode); Task 5 adds the session.
- `doc/v1.1/M13b-console-nodes.md` (forwarding: `TARGET`, `FORWARDED_BY`, the caller's
  credentials), `M13f-console-stream.md` (the stream; its deviations' notes for M14).

## Global Constraints

- Toolchain `1.100.0`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`.
- **Dependency direction** (roadmap §2): `data-avalanche (bin) → api → cluster → pipeline → core,
  config, state, storage`. `testkit` is only ever a dev-dependency.
- **Served while the console is on** (Admin_Console §3): with `globals.console.enabled: false`
  every `/ui/` route answers `404`, and the API takes no session cookie. The switch is read at
  each request, so a reload turns it on or off.
- **Tokens and passwords never reach page JavaScript** (Admin_Console §9): the session's cookies
  are `HttpOnly`; the pages run no inline script or style.
- **The role and the recorded name come from verification** (Admin_Console §9): a session is
  verified again on every request, as the API verifies credentials; nothing it holds is trusted
  without that.
- **No node-to-node trust** (Admin_Console §10, D-M13b.4): a node passes the caller's own session
  on to a peer, which opens and verifies it with the key every node has.
- **The session key never leaves configuration** (config §6.8): it is never stored, logged or
  sent; the tests make their keys at run time.
- **Test gates:** plain `cargo test` stays hermetic: the tests' identity provider runs in the test
  process on loopback. The E2E suite needs `DATA_AVALANCHE_IT=1` and the `e2e-hooks` feature, and
  runs with `--test-threads=1`, on PostgreSQL and on `DATA_AVALANCHE_E2E_SOURCE=mysql` (CI's
  e2e-mysql runs `mysql-84`, `mysql-84-nogtid` and `mariadb-118`).
- **The site follows the behaviour in the same milestone** (`doc/v1.0/README.md`).
- **Public-API conventions:** public types and constants are re-exported at the crate root;
  errors are `thiserror` types.
- **Code blocks:**
  - They are the verified content, already `rustfmt`-formatted.
  - Every block is a `git apply`-ready diff, one per file.
  - A step's diffs apply in the order given, after the earlier steps'.
  - The OpenAPI document, the site's generated reference pages and `Cargo.lock` are regenerated,
    not written (Tasks 1, 5).
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to hurt someone running 1.1.0 in production. Each is pinned by a
test in the task after the arrow.

1. **A provider that rotates refresh tokens**, with several requests refreshing one session at
   once (two tabs, a page and its stream). A refresh token used twice is refused, and some
   providers then end the whole sign-in → Task 4
   (`a_session_whose_token_is_about_to_expire_is_refreshed_once`: one node makes one refresh and
   gives its outcome to the same token for 30 s).
2. **A session that should have ended**: a password changed, a session key rotated, `auth`
   changed, the console switched off, `session_ttl` shortened → Task 1
   (`a_session_ends_with_its_password_its_key_or_its_mode`, `a_session_ends_its_ttl_after_sign_in`).
3. **Another site acting through a signed-in browser**: a forged mutating API call, a sign-in or
   sign-out posted from elsewhere, a `next` that leads off-site → Task 1
   (`a_session_s_requests_that_change_something_send_its_anti_forgery_token`), Task 3
   (`a_form_another_site_posts_is_refused`, `next_is_always_a_console_page`).
4. **A page left open past its session**: its stream would go on sending the cluster's changes →
   Task 2 (`a_stream_ends_when_its_session_does`).
5. **Password guessing at the `basic` form** → Task 3
   (`wrong_passwords_leave_fewer_attempts_then_a_wait`,
   `failures_are_counted_by_address_and_a_success_starts_again`).

## Decisions and rulings

Settled with Oleksii before this plan (2026-11-24):
- a session lasts `session_ttl` **from sign-in**, however it is used (the mockup's "8 h after
  sign-in"; the config spec said "without activity": S-M14.1 amends it);
- the sign-in and error pages are **rendered by the server** now, from the mockup; M15's app links
  to them;
- failed `basic` sign-ins are counted **per client address**.

The rest are Claude's calls.

- **D-M14.1 What this milestone serves, and what waits.**
  - Served here: sign-in under every mode, sessions, the 401 and 403 pages, `none`'s banner, the
    session's JSON for the app (`GET /ui/session`), and its refresh (`POST /ui/session/refresh`).
  - Until M15's app, any other page under `/ui/` shows who is signed in, with the sign-out. M15
    serves the app there. Its calls to `/v1` that answer `401` reload the page, which shows the 401
    page; it refreshes ahead of the token's expiry with `POST /ui/session/refresh`.
  - The Playwright runs of every sign-in mode (Admin_Console §10) come with the app in M15. Here,
    Rust tests drive every route, and the E2E scenario drives `basic` and `oauth2` on real nodes.
- **D-M14.2 The session cookie** (`session.rs`).
  - `da_session`: AES-256-GCM under a key derived from `session_key` (BLAKE3 `derive_key`), a
    random nonce for each seal, the cookie's purpose as associated data. The key's id (8 bytes,
    derived too) goes beside the ciphertext, so a changed key is told from an altered cookie.
  - `HttpOnly; Secure; SameSite=Strict; Path=/`, `Max-Age` until a day after the session ends
    (`KEPT_AFTER`), so an ended session's page can say when it ended.
  - A session over `COOKIE_CHUNK` (3800) bytes goes on in `da_session.1` … `.3` (`MAX_CHUNKS`, 4);
    a longer one starts no session (`TooLong`). A provider's long tokens (Entra's) fit in two.
  - It holds `signed_in` (Unix seconds), the anti-forgery token (32 random bytes), and `by`:
    `basic` (the user, a keyed BLAKE3 fingerprint of the password) or `oauth2` (the access token,
    the refresh token if any). Nothing is kept server-side.
- **D-M14.3 Verified on every request** (`Auth::verify_caller`).
  - The cookie is opened only when the request has no `Authorization`, the console is on, and
    `auth` is `basic` or `oauth2` with a session key.
  - A session ends `session_ttl` after `signed_in`, at the `session_ttl` of the running
    generation: a reload that shortens it ends older sessions at once.
  - `basic`: the user and the password's fingerprint must match the configured ones (a changed
    password ends every session). `oauth2`: the access token verifies as a bearer token does;
    its expiry, or the session's end if sooner, is `Principal::expires`.
  - Another key (`the session key changed`), an altered cookie, another mode (`the cluster's
    sign-in changed`): 401.
  - A token whose roles map to nothing is refused naming the roles it carried
    (`the token's roles (cdc-ops, reporting) name no role this node maps`).
- **D-M14.4 The API takes the session** (`lib.rs`, `openapi.rs`). The console's app calls `/v1`
  with its cookie. A request the session authenticates that isn't `GET`, `HEAD` or `OPTIONS`
  sends the session's anti-forgery token in `X-CSRF-Token` (`CSRF_HEADER`), compared in constant
  time: `403` without it, before its role is looked at, and not recorded as a refused attempt.
  The OpenAPI document lists the cookie as the `session` scheme.
- **D-M14.5 Peers open the session** (M13f's first carry-in). `Presented` (the caller's
  `Authorization`, its session cookies alone, its anti-forgery token) replaces the
  `Authorization`-only forwarding of node actions, another node's detail and a stream's follows.
  It never prints.
- **D-M14.6 A stream ends with its credentials** (M13f's second carry-in). An open stream checks
  its caller every `CREDENTIALS_EVERY` (60 s), and a second after `Principal::expires` when that
  comes first. Once they no longer verify, it sends `reset` (`credentials`) and ends, and the page
  opens it again. A verifier that can't check (503) refuses no one, as in M13f.
- **D-M14.7 The `basic` sign-in.** A form posted to `POST /ui/sign-in` (`user`, `password`,
  `next`), checked as the API checks basic credentials. `SIGN_IN_ATTEMPTS` (5) failures in a row
  from one address make it wait `SIGN_IN_WAIT` (60 s) on that node; a success starts the count
  again. Behind a proxy every client shares the proxy's address; forwarded headers aren't trusted.
  At most 10 000 addresses are kept.
- **D-M14.8 The `oauth2` sign-in** (`oauth2.rs`).
  - The endpoints come from the discovery document (`discovery_url`, else
    `<issuer>/.well-known/openid-configuration`), whose `issuer` must be the configured one; cached
    10 min; no redirect followed, a body over 1 MiB refused.
  - `GET /ui/oauth2/start`: a new state and PKCE verifier (`S256`), sealed in `da_oauth2`
    (`SameSite=Lax`, `Path=/ui/oauth2/callback`, `SIGN_IN_WITHIN` 10 min), then the provider.
  - `GET /ui/oauth2/callback`: the state checked in constant time, the code exchanged
    (`client_secret_basic`, or `client_secret_post` when that is all the provider lists; a public
    client sends its `client_id`), the access token verified before a session starts. The answer
    is a page that goes on to `next` from the console's origin: a `Strict` cookie isn't sent on a
    redirect the provider began. A failure shows the sign-in page with why, without the countdown.
  - The ID token, sealed in `da_id` (`Path=/ui/sign-out`), is the sign-out's `id_token_hint`.
  - Refresh: a page asked for refreshes a session whose token expires within `REFRESH_AHEAD`
    (60 s); the app refreshes with `POST /ui/session/refresh` and the anti-forgery token. A
    refresh keeps `signed_in` and the anti-forgery token, so the session ends when it would have.
    One node refreshes one refresh token at a time and gives the outcome to the same token for
    30 s (Review Focus 1); two nodes refreshing the same session at once aren't
    coordinated.
  - The sign-in page names the provider by `idp_name`, else the issuer's host, and goes there by
    itself after `AUTO_REDIRECT` (5 s, `sign-in.js`) unless it follows a sign-out (`signed_out`),
    a failure, or the person stays (`stay`).
- **D-M14.9 The pages and their hardening** (`pages.rs`, `console.rs`).
  - Every `/ui/` answer: `Content-Security-Policy: default-src 'none'; script-src 'self';
    style-src 'self'; img-src 'self' data:; connect-src 'self'; form-action 'self' <provider's
    origins>; frame-ancestors 'none'; base-uri 'none'`, `X-Frame-Options: DENY`,
    `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, `Cache-Control: no-store`
    (the assets `no-cache`). `form-action` adds the issuer's origin and the end-session
    endpoint's, where the sign-out goes on.
  - A page asked for with no credentials goes to the sign-in (`303`, `next` the page). With
    credentials that no longer verify it is "Your session has ended" (401) with why and "Sign in
    again"; with `jwt`, "No valid token" (401) with no button; with no role mapped, "Forbidden"
    (403); when nothing can be verified, 503.
  - A form posted to `/ui/` from another site is refused (`Sec-Fetch-Site` other than
    `same-origin` or `none`; else an `Origin` whose host isn't the request's `Host`); a client
    that says neither (`curl`) is taken at its word. `next` is a path under `/ui/` or `/ui/`.
  - The footer names the node and the version. The mockup's "TLS ✓" waits for M15: the router
    doesn't know whether a proxy in front terminates TLS.
- **D-M14.10 The sign-out** (`POST /ui/sign-out`, from the console's own pages) removes every
  session cookie (and `da_id`), then goes to the sign-in (`signed_out`), or with `oauth2` to the
  provider's end-session endpoint (`client_id`, `post_logout_redirect_uri` =
  `<public_url>/ui/sign-in?signed_out`, `id_token_hint`). Nothing is kept server-side, so a copy
  of the cookie taken before the sign-out works until the session ends; the site says so.
- **D-M14.11 Rollback.** Nothing is stored. A node rolled back to 1.0.x ignores the cookies; a
  roll forward again opens them while the key is the same. No release note.
- **D-M14.12 The E2E scenario** runs `basic` and `oauth2` (a provider in the test's process) on
  three nodes, about 35 s on each source.

### Spec patch S-M14.1 (needs approval)

1. `doc/specs/Configuration_Specification.md` §6.8, the `session_ttl` row:

   > | `session_ttl` | duration | no | `8h` | R | How long a session lasts after sign-in, however it is used (`basic`, `oauth2`); with `oauth2`, never past the token's expiry unless the provider refreshes it. 5 min – 7 days (V-132). |

   (was: "How long a session lasts without activity (`basic`, `oauth2`); …")

2. `doc/specs/System-Overview.md` §Web console, two bullets after "off by default; …":

   > - people sign in as `globals.api.auth` says: a form (`basic`) or the identity provider's sign-in (`oauth2`, authorization code with PKCE), each starting a session in an encrypted cookie every node reads, which lasts `globals.console.session_ttl` from sign-in; `jwt` takes a bearer token on every request, from a proxy in front; `none` lets everyone in as `admin`, with a banner
   > - the management API takes the console's session as it takes credentials; a request the session authenticates that changes something carries the session's anti-forgery token (`X-CSRF-Token`)

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-api/src/session.rs` (new) | The session cookie: `Sealer`, `Session`, `By`, chunking, `Presented`'s cookies (Task 1); a session's removal (Task 3) |
| `crates/data-avalanche-api/src/auth.rs` | Sessions opened and verified (`verify_caller`, `Verified`, `Sessions`, `Presented`, `Principal::expires`, `CSRF_HEADER`) (Task 1); `SignIn`, `basic_matches`, the attempts, the 403 naming the roles (Task 3); `verify_token`, the provider (Task 4) |
| `crates/data-avalanche-api/src/{peers,follow,stream}.rs` | The caller's session passed on to peers (Task 1); a stream ends with its credentials (Task 2) |
| `crates/data-avalanche-api/src/lib.rs`, `openapi.rs`, `openapi.json` | The anti-forgery check in the gate; the `session` scheme (Task 1); the console's routes (Task 3) |
| `crates/data-avalanche-api/src/console.rs` (new), `pages.rs` (new), `assets/console.css` (new), `server.rs` | The routes under `/ui/`, the pages, the client's address (Task 3) |
| `crates/data-avalanche-api/src/oauth2.rs` (new), `assets/sign-in.js` (new) | The `oauth2` sign-in (Task 4) |
| `crates/data-avalanche-api/src/{test_support,actions,nodes}.rs` | Consoles, sessions and a provider for the tests (Tasks 1, 4) |
| `crates/data-avalanche/src/run.rs`, `Cargo.toml`, `crates/data-avalanche-api/Cargo.toml` | `Auth` takes `globals.console`; `aws-lc-rs` (Task 1) |
| `crates/data-avalanche-config/src/field_docs.rs`, `docs/…/deployment/console.md` (new), `docs/…/reference/configuration.md`, `doc/research/{Admin_Console,API_RBAC_and_Versioning}.md` | The site and the research docs (Task 5) |
| `crates/data-avalanche/tests/e2e/{console_sessions,harness,idp,main}.rs` | The scenario on three nodes (Task 6) |

The plan's commit also carries S-M14.1 (once approved) and `doc/v1.1/README.md`: M13f merged
(#32), and this plan's row.

---

### Task 1: Sessions every node opens (`data-avalanche-api`, `data-avalanche`)

**Files:**
- Regenerate: `Cargo.lock`
- Modify: `Cargo.toml`
- Modify: `crates/data-avalanche-api/Cargo.toml`
- Regenerate: `crates/data-avalanche-api/openapi.json`
- Modify: `crates/data-avalanche-api/src/actions.rs`
- Modify: `crates/data-avalanche-api/src/auth.rs`
- Modify: `crates/data-avalanche-api/src/follow.rs`
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Modify: `crates/data-avalanche-api/src/nodes.rs`
- Modify: `crates/data-avalanche-api/src/openapi.rs`
- Modify: `crates/data-avalanche-api/src/peers.rs`
- Create: `crates/data-avalanche-api/src/session.rs`
- Modify: `crates/data-avalanche-api/src/stream.rs`
- Modify: `crates/data-avalanche-api/src/test_support.rs`
- Modify: `crates/data-avalanche/src/run.rs`
- Regenerate: `docs/public/openapi.json`

**Interfaces:**
- Consumes: `Auth` and its token verifier (M11b); `globals.console` (`Console`: `enabled`,
  `session_key`, `session_ttl`, `username_claim`, config §6.8); `Asked` (M13b); `Follow`,
  `Peers::follow` (M13f).
- Produces (crate root): `SESSION_COOKIE` (`"da_session"`), `COOKIE_CHUNK` (3800), `MAX_CHUNKS`
  (4), `KEPT_AFTER` (24 h), `CSRF_HEADER` (`"x-csrf-token"`); `Auth::new(&ApiConfig, &Console)`,
  `Auth::set(&ApiConfig, &Console)`; `Principal { name, role, expires: Option<DateTime<Utc>> }`.
- Produces (`session.rs`, crate-private):
  - `Sealer::{new(session_key: &str), seal(&self, purpose, plain) -> String, open(&self, purpose,
    sealed) -> Result<Vec<u8>, Unsealed>, fingerprint(&self, secret) -> String}`;
    `Unsealed::{KeyChanged, Garbled}`; `random_token() -> String`;
  - `Session { signed_in: i64, csrf: String, by: By }`, `By::{Basic { user, fingerprint },
    Oauth2 { access, refresh: Option<String> }}`; `Session::new(by, signed_in)`,
    `Session::cookies(&self, &Sealer, max_age: u64) -> Result<Vec<String>, TooLong>`,
    `Session::of(&HeaderMap, &Sealer) -> Option<Result<Session, Unsealed>>`;
  - `cookie(&HeaderMap, name)`, `sealed_value`, `session_cookies(&HeaderMap) -> Option<String>`,
    `set_cookie(name, value, path, max_age, same_site)`, `removed(name, path)`.
- Produces (`auth.rs`, crate-private): `Auth::verify_caller(&HeaderMap) -> Result<Verified,
  Denied>`, `Verified { principal, session: Option<Session> }`; `Sessions { sealer, ttl }`,
  `Sessions::ends(signed_in) -> DateTime<Utc>`; `Presented::{of(&HeaderMap), headers()}`;
  `span(Duration) -> String`.
- Tests (`test_support.rs`): `console`, `console_naming`, `SESSION_KEY`, `sessions`, `basic_api`,
  `session_cookie`, `basic_session`, `presented`, `Issuer::oauth2_api`; `Seen` records each
  forwarded request's `cookie` and `csrf`.

- [ ] **Step 1: Write the failing tests**

The tests:
- `a_session_sealed_by_one_node_opens_on_another_with_the_same_key`,
  `a_session_key_changed_is_told_from_a_cookie_altered`,
  `a_value_sealed_for_one_purpose_doesnt_open_as_another`,
  `a_long_session_takes_several_cookies_and_too_long_a_one_none`,
  `the_session_cookies_alone_are_passed_on`,
  `a_password_s_fingerprint_depends_on_the_key_and_the_password`,
  `a_session_s_tokens_never_print` (session);
- `a_basic_session_verifies_on_every_node_with_the_key`, `a_session_ends_its_ttl_after_sign_in`,
  `a_session_ends_with_its_password_its_key_or_its_mode`,
  `an_oauth2_session_verifies_its_token_as_a_bearer_token_is`,
  `what_a_caller_presented_never_prints` (auth);
- `a_session_s_requests_that_change_something_send_its_anti_forgery_token` (the routes);
- `a_node_action_is_forwarded_with_the_callers_session` (nodes).

The tests that build an `Auth` pass a `Console`; the forwarding and following tests pass a
`Presented`. The new module holds only its tests for now.

`crates/data-avalanche-api/src/actions.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/actions.rs b/crates/data-avalanche-api/src/actions.rs
index 18f3dde..fd0644c 100644
--- a/crates/data-avalanche-api/src/actions.rs
+++ b/crates/data-avalanche-api/src/actions.rs
@@ -328,9 +328,7 @@ mod tests {
     use tempfile::TempDir;
 
     use super::*;
-    use crate::test_support::{
-        CONFIG, Issuer, Lone, USERNAME_CLAIM, api, lone, open, request, serve,
-    };
+    use crate::test_support::{CONFIG, Issuer, Lone, api, console, lone, open, request, serve};
     use crate::{Api, Auth};
 
     /// The lone node's API under `auth`, its pipeline `orders` known.
@@ -485,8 +483,7 @@ mod tests {
     #[tokio::test]
     async fn a_job_s_actions_name_the_job_and_its_pipeline() {
         let idp = Issuer::start().await;
-        let (addr, lone, _dir) =
-            served(Arc::new(Auth::new(&idp.api("roles"), USERNAME_CLAIM))).await;
+        let (addr, lone, _dir) = served(Arc::new(Auth::new(&idp.api("roles"), &console()))).await;
         let config = lone.node.coordinator.config().await;
         let docs = (lone.node.docs)(&config.pipelines["orders"]).unwrap();
         let mut job = data_avalanche_state::SnapshotJobDoc::new(
@@ -562,8 +559,7 @@ mod tests {
     #[tokio::test]
     async fn a_caller_below_the_role_is_recorded_as_denied() {
         let idp = Issuer::start().await;
-        let (addr, lone, _dir) =
-            served(Arc::new(Auth::new(&idp.api("roles"), USERNAME_CLAIM))).await;
+        let (addr, lone, _dir) = served(Arc::new(Auth::new(&idp.api("roles"), &console()))).await;
         let token = format!("Bearer {}", idp.token(serde_json::json!(["viewer"])));
         let auth = [("Authorization", token.as_str())];
         let (code, _, _) = request(addr, "POST", "/v1/admin/shutdown", &auth, "").await;
@@ -619,8 +615,7 @@ mod tests {
     #[tokio::test]
     async fn a_token_with_no_mapped_role_is_recorded_as_denied() {
         let idp = Issuer::start().await;
-        let (addr, lone, _dir) =
-            served(Arc::new(Auth::new(&idp.api("roles"), USERNAME_CLAIM))).await;
+        let (addr, lone, _dir) = served(Arc::new(Auth::new(&idp.api("roles"), &console()))).await;
         let token = format!("Bearer {}", idp.token(serde_json::json!(["cdc-reader"])));
         let auth = [("Authorization", token.as_str())];
         let (code, _, body) = request(addr, "POST", "/v1/pipelines/orders/pause", &auth, "").await;
@@ -730,8 +725,7 @@ mod tests {
     #[tokio::test]
     async fn a_flood_of_refused_calls_is_recorded_for_a_burst_only() {
         let idp = Issuer::start().await;
-        let (addr, lone, _dir) =
-            served(Arc::new(Auth::new(&idp.api("roles"), USERNAME_CLAIM))).await;
+        let (addr, lone, _dir) = served(Arc::new(Auth::new(&idp.api("roles"), &console()))).await;
         let token = format!("Bearer {}", idp.token(serde_json::json!(["viewer"])));
         let auth = [("Authorization", token.as_str())];
         let started = std::time::Instant::now();
````

`crates/data-avalanche-api/src/auth.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/auth.rs b/crates/data-avalanche-api/src/auth.rs
index 87c8c90..f9af2cd 100644
--- a/crates/data-avalanche-api/src/auth.rs
+++ b/crates/data-avalanche-api/src/auth.rs
@@ -619,7 +619,11 @@ mod tests {
     use serde_json::json;
 
     use super::*;
-    use crate::test_support::{Issuer, USERNAME_CLAIM, config, sign_with};
+    use crate::session::{By, Session};
+    use crate::test_support::{
+        Issuer, basic_api, basic_session, config, console, console_naming, session_cookie,
+        sessions, sign_with,
+    };
 
     fn bearer(token: &str) -> HeaderMap {
         let mut h = HeaderMap::new();
@@ -693,7 +697,7 @@ mod tests {
 
     #[tokio::test]
     async fn without_auth_every_caller_is_admin() {
-        let auth = Auth::new(&config().globals.api, USERNAME_CLAIM);
+        let auth = Auth::new(&config().globals.api, &console());
         let p = auth.verify(&HeaderMap::new()).await.unwrap();
         assert_eq!((p.name.as_str(), p.role), ("anonymous", Role::Admin));
     }
@@ -730,7 +734,7 @@ mod tests {
         };
         let token = bearer(&idp.token(json!(["admin"])));
         for api in [jwt_without, basic_without, oauth2_without_urls] {
-            let auth = Auth::new(&api, USERNAME_CLAIM);
+            let auth = Auth::new(&api, &console());
             for headers in [HeaderMap::new(), token.clone()] {
                 let why = unavailable(auth.verify(&headers).await);
                 assert!(why.contains("inconsistent"), "{:?}: {why}", api.auth);
@@ -746,7 +750,7 @@ mod tests {
             user: "ops".into(),
             password: Secret::new("s3cret", &FingerprintKey::from_bytes([1; 32])),
         });
-        let auth = Auth::new(&api, USERNAME_CLAIM);
+        let auth = Auth::new(&api, &console());
         let with = |user_pass: &str| {
             let mut h = HeaderMap::new();
             let v = format!("basic {}", STANDARD.encode(user_pass));
@@ -767,7 +771,7 @@ mod tests {
     #[tokio::test]
     async fn a_token_s_highest_mapped_role_is_the_caller_s() {
         let idp = Issuer::start().await;
-        let auth = Auth::new(&idp.api("roles"), USERNAME_CLAIM);
+        let auth = Auth::new(&idp.api("roles"), &console());
         let role = |roles| {
             let token = idp.token(roles);
             let auth = &auth;
@@ -792,7 +796,7 @@ mod tests {
             (Role::Viewer, vec!["readonly".into()]),
             (Role::Admin, vec!["cdc-admin".into()]),
         ]);
-        let auth = Auth::new(&api, USERNAME_CLAIM);
+        let auth = Auth::new(&api, &console());
         let realm = |roles| {
             let mut claims = idp.claims(json!(null));
             claims["realm_access"] = json!({ "roles": roles });
@@ -817,7 +821,7 @@ mod tests {
         let idp = Issuer::start().await;
         let named = |claims: &Value, username_claim: &str| {
             let (auth, token) = (
-                Auth::new(&idp.api("roles"), username_claim),
+                Auth::new(&idp.api("roles"), &console_naming(username_claim)),
                 bearer(&idp.sign("k1", claims)),
             );
             async move { auth.verify(&token).await.unwrap().name }
@@ -837,8 +841,8 @@ mod tests {
             "not a string"
         );
         // A reload's claim is the one taken.
-        let auth = Auth::new(&idp.api("roles"), "preferred_username");
-        auth.set(&idp.api("roles"), "email");
+        let auth = Auth::new(&idp.api("roles"), &console_naming("preferred_username"));
+        auth.set(&idp.api("roles"), &console_naming("email"));
         let p = auth.verify(&bearer(&idp.sign("k1", &claims))).await;
         assert_eq!(p.unwrap().name, "oleksii@example.com");
         // A token with no role this node maps is refused under its name.
@@ -855,7 +859,7 @@ mod tests {
     async fn a_namespaced_roles_claim_is_read_by_its_name() {
         let idp = Issuer::start().await;
         let claim = "https://example.com/roles";
-        let auth = Auth::new(&idp.api(claim), USERNAME_CLAIM);
+        let auth = Auth::new(&idp.api(claim), &console());
         let mut claims = idp.claims(json!(null));
         claims[claim] = json!(["operator"]);
         let p = auth.verify(&bearer(&idp.sign("k1", &claims))).await;
@@ -865,7 +869,7 @@ mod tests {
     #[tokio::test]
     async fn a_token_that_doesnt_verify_is_refused() {
         let idp = Issuer::start().await;
-        let auth = Auth::new(&idp.api("roles"), USERNAME_CLAIM);
+        let auth = Auth::new(&idp.api("roles"), &console());
         let now = jsonwebtoken::get_current_timestamp();
         let with = |f: &dyn Fn(&mut Value)| {
             let mut c = idp.claims(json!(["admin"]));
@@ -944,7 +948,7 @@ mod tests {
     #[tokio::test]
     async fn unknown_key_ids_fetch_at_most_once_a_refetch_interval() {
         let idp = Issuer::start().await;
-        let auth = Auth::new(&idp.api("roles"), USERNAME_CLAIM);
+        let auth = Auth::new(&idp.api("roles"), &console());
         auth.verify(&bearer(&idp.token(json!(["viewer"]))))
             .await
             .unwrap();
@@ -974,16 +978,13 @@ mod tests {
         };
         api.oauth2 = Some(oauth2(&idp.url));
         let token = bearer(&idp.token(json!(["operator"])));
-        let p = Auth::new(&api, USERNAME_CLAIM)
-            .verify(&token)
-            .await
-            .unwrap();
+        let p = Auth::new(&api, &console()).verify(&token).await.unwrap();
         assert_eq!(p.role, Role::Operator);
 
         // A discovery document of another issuer is not this one's.
         api.oauth2 = Some(oauth2("https://idp.example.com"));
         // The reason is logged; the caller isn't told the URL or the error.
-        let why = unavailable(Auth::new(&api, USERNAME_CLAIM).verify(&token).await);
+        let why = unavailable(Auth::new(&api, &console()).verify(&token).await);
         assert_eq!(why, "the token issuer's keys can't be fetched");
         assert!(!why.contains(&idp.url), "{why}");
     }
@@ -997,7 +998,7 @@ mod tests {
         let url = format!("http://{}/jwks", closed.local_addr().unwrap());
         drop(closed);
         api.jwt.as_mut().unwrap().jwks_url = url;
-        let auth = Auth::new(&api, USERNAME_CLAIM);
+        let auth = Auth::new(&api, &console());
         let token = bearer(&idp.token(json!(["admin"])));
         // Again, inside the refetch window: still no keys, still 503.
         for _ in 0..2 {
@@ -1013,7 +1014,7 @@ mod tests {
         let token = bearer(&idp.token(json!(["viewer"])));
 
         // Young keys are trusted: the withdrawal isn't heard yet.
-        let young = Auth::new(&api, USERNAME_CLAIM);
+        let young = Auth::new(&api, &console());
         young.verify(&token).await.unwrap();
         idp.remove_key("k1");
         idp.add_key("k2");
@@ -1152,7 +1153,7 @@ mod tests {
         let idp = Issuer::start().await;
         let mut api = idp.api("roles");
         api.jwt.as_mut().unwrap().audience = None;
-        let auth = Auth::new(&api, USERNAME_CLAIM);
+        let auth = Auth::new(&api, &console());
         let mut claims = idp.claims(json!(["operator"]));
         claims.as_object_mut().unwrap().remove("aud");
         let p = auth.verify(&bearer(&idp.sign("k1", &claims))).await;
@@ -1163,13 +1164,13 @@ mod tests {
     async fn a_reload_of_the_role_mapping_keeps_the_keys() {
         let idp = Issuer::start().await;
         let api = idp.api("roles");
-        let auth = Auth::new(&api, USERNAME_CLAIM);
+        let auth = Auth::new(&api, &console());
         let token = bearer(&idp.token(json!(["viewer"])));
         assert_eq!(auth.verify(&token).await.unwrap().role, Role::Viewer);
         idp.set_down(true);
         let mut mapped = api.clone();
         mapped.role_mapping = BTreeMap::from([(Role::Admin, vec!["viewer".into()])]);
-        auth.set(&mapped, USERNAME_CLAIM);
+        auth.set(&mapped, &console());
         let p = auth.verify(&token).await;
         assert_eq!(
             p.unwrap().role,
@@ -1184,17 +1185,17 @@ mod tests {
     async fn a_reload_keeps_the_keys_unless_their_source_changes() {
         let idp = Issuer::start().await;
         let api = idp.api("roles");
-        let auth = Auth::new(&api, USERNAME_CLAIM);
+        let auth = Auth::new(&api, &console());
         let token = bearer(&idp.token(json!(["viewer"])));
         auth.verify(&token).await.unwrap();
         let mut moved = api.clone();
         moved.address.port = 9_999;
-        auth.set(&moved, USERNAME_CLAIM);
+        auth.set(&moved, &console());
         auth.verify(&token).await.unwrap();
         assert_eq!(idp.fetches.load(Ordering::SeqCst), 1);
         let mut other = api.clone();
         other.jwt.as_mut().unwrap().audience = Some("another-api".into());
-        auth.set(&other, USERNAME_CLAIM);
+        auth.set(&other, &console());
         denied_401(auth.verify(&token).await);
         assert_eq!(
             idp.fetches.load(Ordering::SeqCst),
@@ -1203,13 +1204,197 @@ mod tests {
         );
         let mut moved = api.clone();
         moved.jwt.as_mut().unwrap().jwks_url = format!("{}/jwks?again", idp.url);
-        auth.set(&moved, USERNAME_CLAIM);
+        auth.set(&moved, &console());
         auth.verify(&token).await.unwrap();
         assert_eq!(idp.fetches.load(Ordering::SeqCst), 2, "another JWKS URL");
-        auth.set(&config().globals.api, USERNAME_CLAIM);
+        auth.set(&config().globals.api, &console());
         assert_eq!(
             auth.verify(&HeaderMap::new()).await.unwrap().role,
             Role::Admin
         );
     }
+
+    fn with_cookie(cookie: &str) -> HeaderMap {
+        let mut h = HeaderMap::new();
+        h.insert(COOKIE, HeaderValue::from_str(cookie).unwrap());
+        h
+    }
+
+    const HOUR: Duration = Duration::from_secs(3600);
+
+    /// Admin_Console §9: every node opens a session with the key every node has, and verifies its
+    /// person again as it verifies basic credentials.
+    #[tokio::test]
+    async fn a_basic_session_verifies_on_every_node_with_the_key() {
+        let session = basic_session("s3cret", Duration::from_secs(60));
+        let ends = DateTime::from_timestamp(session.signed_in, 0).unwrap() + HOUR;
+        let cookie = with_cookie(&format!("theme=dark; {}", session_cookie(&session)));
+        for _node in 0..2 {
+            let auth = Auth::new(&basic_api("s3cret"), &sessions(HOUR));
+            let v = auth.verify_caller(&cookie).await.unwrap();
+            assert_eq!(
+                v.principal,
+                Principal {
+                    name: "ops".into(),
+                    role: Role::Admin,
+                    expires: Some(ends),
+                }
+            );
+            assert_eq!(v.session, Some(session.clone()));
+        }
+        // Credentials sent beside it are the ones taken.
+        let mut both = cookie.clone();
+        both.insert(
+            AUTHORIZATION,
+            HeaderValue::from_static("Basic b3BzOm5vcGU="),
+        );
+        let auth = Auth::new(&basic_api("s3cret"), &sessions(HOUR));
+        assert_eq!(
+            denied_401(auth.verify(&both).await),
+            "wrong user or password"
+        );
+    }
+
+    /// S-M14.1: a session ends `session_ttl` after sign-in, however it was used.
+    #[tokio::test]
+    async fn a_session_ends_its_ttl_after_sign_in() {
+        let auth = Auth::new(&basic_api("s3cret"), &sessions(8 * HOUR));
+        let old = basic_session("s3cret", 8 * HOUR + Duration::from_secs(1));
+        let why = denied_401(auth.verify(&with_cookie(&session_cookie(&old))).await);
+        let ends = DateTime::from_timestamp(old.signed_in, 0).unwrap() + 8 * HOUR;
+        assert_eq!(
+            why,
+            format!(
+                "the session expired at {} (8 h after sign-in)",
+                ends.to_rfc3339_opts(SecondsFormat::Secs, true)
+            )
+        );
+        // A shorter `session_ttl`, reloaded, ends older sessions at once.
+        let young = basic_session("s3cret", 2 * HOUR);
+        let cookie = with_cookie(&session_cookie(&young));
+        auth.verify(&cookie).await.unwrap();
+        auth.set(&basic_api("s3cret"), &sessions(HOUR));
+        assert!(denied_401(auth.verify(&cookie).await).contains("(1 h after sign-in)"));
+    }
+
+    /// Config §6.8: a password or a session key changed, the sign-in's mode changed, or the
+    /// console switched off, and a session no longer lets its person in.
+    #[tokio::test]
+    async fn a_session_ends_with_its_password_its_key_or_its_mode() {
+        let cookie = with_cookie(&session_cookie(&basic_session("s3cret", HOUR / 2)));
+        let auth = Auth::new(&basic_api("s3cret"), &sessions(HOUR));
+        auth.verify(&cookie).await.unwrap();
+        auth.set(&basic_api("rotated"), &sessions(HOUR));
+        assert_eq!(
+            denied_401(auth.verify(&cookie).await),
+            "the user or the password changed since the sign-in"
+        );
+        let mut rekeyed = sessions(HOUR);
+        rekeyed.session_key = Some(Secret::new(
+            "another session key, of 32 bytes or more",
+            &FingerprintKey::from_bytes([1; 32]),
+        ));
+        auth.set(&basic_api("s3cret"), &rekeyed);
+        assert_eq!(
+            denied_401(auth.verify(&cookie).await),
+            "the session key changed"
+        );
+        auth.set(&basic_api("s3cret"), &sessions(HOUR));
+        assert_eq!(
+            denied_401(auth.verify(&with_cookie("da_session=AAAA")).await),
+            "the session cookie doesn't decrypt"
+        );
+        // The console off: the cookie isn't looked at.
+        auth.set(&basic_api("s3cret"), &console());
+        assert_eq!(
+            denied_401(auth.verify(&cookie).await),
+            "basic credentials are required"
+        );
+        // Another mode: a basic session isn't an oauth2 one.
+        let idp = Issuer::start().await;
+        auth.set(&idp.oauth2_api("roles"), &sessions(HOUR));
+        assert_eq!(
+            denied_401(auth.verify(&cookie).await),
+            "the cluster's sign-in changed since the session began"
+        );
+        // `jwt` and `none` have no sessions.
+        for api in [idp.api("roles"), config().globals.api] {
+            auth.set(&api, &sessions(HOUR));
+            assert!(
+                auth.verify_caller(&cookie)
+                    .await
+                    .map_or(true, |v| v.session.is_none())
+            );
+        }
+    }
+
+    /// Admin_Console §9: an oauth2 session's role and name come from its access token, verified as
+    /// a bearer token is; it lasts while both the token and the session do.
+    #[tokio::test]
+    async fn an_oauth2_session_verifies_its_token_as_a_bearer_token_is() {
+        let idp = Issuer::start().await;
+        let auth = Auth::new(&idp.oauth2_api("roles"), &sessions(8 * HOUR));
+        let session_of = |claims: &Value| {
+            Session::new(
+                By::Oauth2 {
+                    access: idp.sign("k1", claims),
+                    refresh: None,
+                },
+                Utc::now().timestamp() - 60,
+            )
+        };
+        let mut claims = idp.claims(json!(["operator"]));
+        claims["preferred_username"] = json!("alice.smith");
+        let cookie = with_cookie(&session_cookie(&session_of(&claims)));
+        let p = auth.verify(&cookie).await.unwrap();
+        let exp = claims["exp"].as_i64().unwrap();
+        assert_eq!(
+            (p.name.as_str(), p.role, p.expires),
+            (
+                "alice.smith",
+                Role::Operator,
+                DateTime::from_timestamp(exp, 0)
+            ),
+            "the token's expiry, before the session's"
+        );
+        // The session's end, when the token outlives it.
+        let short = Auth::new(
+            &idp.oauth2_api("roles"),
+            &sessions(Duration::from_secs(300)),
+        );
+        let s = session_of(&claims);
+        let ends = DateTime::from_timestamp(s.signed_in + 300, 0);
+        let p = short.verify(&with_cookie(&session_cookie(&s))).await;
+        assert_eq!(p.unwrap().expires, ends);
+        // A token expired (past the verifier's minute of leeway) or with no role mapped.
+        let mut expired = claims.clone();
+        expired["exp"] = json!(Utc::now().timestamp() - 120);
+        let why = denied_401(
+            auth.verify(&with_cookie(&session_cookie(&session_of(&expired))))
+                .await,
+        );
+        assert!(why.starts_with("the provider's token: "), "{why}");
+        let mut unmapped = claims.clone();
+        unmapped["roles"] = json!(["cdc-reader"]);
+        assert!(matches!(
+            auth.verify(&with_cookie(&session_cookie(&session_of(&unmapped))))
+                .await,
+            Err(Denied::Forbidden { name, .. }) if name == "alice.smith"
+        ));
+    }
+
+    /// What a peer is passed of a caller's credentials never prints.
+    #[test]
+    fn what_a_caller_presented_never_prints() {
+        let p = crate::test_support::presented(&[
+            ("authorization", "Bearer the-token"),
+            ("cookie", "da_session=the-session"),
+            (CSRF_HEADER, "the-csrf"),
+        ]);
+        let printed = format!("{p:?}");
+        for secret in ["the-token", "the-session", "the-csrf"] {
+            assert!(!printed.contains(secret), "{printed}");
+        }
+        assert_eq!(p.headers().len(), 3);
+    }
 }
````

`crates/data-avalanche-api/src/follow.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/follow.rs b/crates/data-avalanche-api/src/follow.rs
index 02b3ca0..4e2059e 100644
--- a/crates/data-avalanche-api/src/follow.rs
+++ b/crates/data-avalanche-api/src/follow.rs
@@ -191,6 +191,7 @@ mod tests {
     use futures_util::stream;
 
     use super::*;
+    use crate::test_support::presented;
 
     fn frame(kind: &str, data: &str) -> Frame {
         Frame {
@@ -279,7 +280,10 @@ mod tests {
                 "node-b",
                 &url,
                 "/v1/stream?kind=node",
-                Some(HeaderValue::from_static("Basic dmlld2VyOnZpZXdlcg==")),
+                &presented(&[
+                    ("authorization", "Basic dmlld2VyOnZpZXdlcg=="),
+                    ("cookie", "theme=dark; da_session=abc"),
+                ]),
                 silence,
             )
             .await
@@ -293,6 +297,11 @@ mod tests {
         assert_eq!(h[TARGET], "node-b");
         assert_eq!(h[FORWARDED_BY], "node-a");
         assert_eq!(h[header::AUTHORIZATION], "Basic dmlld2VyOnZpZXdlcg==");
+        assert_eq!(
+            h[header::COOKIE],
+            "da_session=abc",
+            "the session's cookie alone"
+        );
     }
 
     #[tokio::test]
@@ -305,7 +314,13 @@ mod tests {
         .await;
         let peers = Peers::new("node-a", None).unwrap();
         let err = peers
-            .follow("node-b", &url, "/v1/stream", None, FOLLOW_SILENCE)
+            .follow(
+                "node-b",
+                &url,
+                "/v1/stream",
+                &Presented::default(),
+                FOLLOW_SILENCE,
+            )
             .await
             .unwrap_err();
         assert_eq!(err, "it answered 404: no pipeline \"x\"");
@@ -314,7 +329,7 @@ mod tests {
                 "node-b",
                 "http://127.0.0.1:9",
                 "/v1/stream",
-                None,
+                &Presented::default(),
                 FOLLOW_SILENCE,
             )
             .await
@@ -333,7 +348,13 @@ mod tests {
         )
         .await;
         let err = peers
-            .follow("node-b", &url, "/v1/stream", None, FOLLOW_SILENCE)
+            .follow(
+                "node-b",
+                &url,
+                "/v1/stream",
+                &Presented::default(),
+                FOLLOW_SILENCE,
+            )
             .await
             .unwrap_err();
         assert_eq!(err, "it answered 502", "a body over the cap isn't read");
@@ -344,7 +365,13 @@ mod tests {
         )
         .await;
         let err = peers
-            .follow("node-b", &url, "/v1/stream", None, FOLLOW_SILENCE)
+            .follow(
+                "node-b",
+                &url,
+                "/v1/stream",
+                &Presented::default(),
+                FOLLOW_SILENCE,
+            )
             .await
             .unwrap_err();
         assert_eq!(
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 89c654d..c4247e0 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -25,6 +25,7 @@ mod pipelines;
 mod prometheus;
 mod publish;
 mod server;
+mod session;
 mod snapshots;
 mod stream;
 mod tables;
@@ -1049,6 +1050,40 @@ mod tests {
         );
     }
 
+    /// Admin_Console §9: a request the console's session authenticates passes the gate as a
+    /// caller with credentials does; one that changes something sends the session's
+    /// anti-forgery token too, and is refused without it, whatever the caller's role.
+    #[tokio::test]
+    async fn a_session_s_requests_that_change_something_send_its_anti_forgery_token() {
+        use crate::test_support::{basic_api, basic_session, session_cookie, sessions};
+        let hour = std::time::Duration::from_secs(3600);
+        let auth = Auth::new(&basic_api("s3cret"), &sessions(hour));
+        let addr = serve(api(Arc::new(auth))).await;
+        let session = basic_session("s3cret", hour / 2);
+        let cookie = session_cookie(&session);
+        for r in routes() {
+            let path = r.path.replace("{id}", "orders");
+            let with = [("Cookie", cookie.as_str())];
+            let (code, _, body) = request(addr, r.method, &path, &with, "").await;
+            let past = |code: u16| code != 401 && code != 403;
+            if r.method == "GET" {
+                assert!(past(code), "{} {path}: {code} {body}", r.method);
+                continue;
+            }
+            assert_eq!(code, 403, "{} {path} without the token", r.method);
+            assert!(body.contains("anti-forgery token"), "{body}");
+            let wrong = [("Cookie", cookie.as_str()), (CSRF_HEADER, "not-it")];
+            let (code, _, _) = request(addr, r.method, &path, &wrong, "").await;
+            assert_eq!(code, 403, "{} {path} with another token", r.method);
+            let right = [
+                ("Cookie", cookie.as_str()),
+                (CSRF_HEADER, session.csrf.as_str()),
+            ];
+            let (code, _, body) = request(addr, r.method, &path, &right, "").await;
+            assert!(past(code), "{} {path}: {code} {body}", r.method);
+        }
+    }
+
     /// Every `/v1` route: 401 without a token or with one that doesn't verify, 403 below its
     /// role or with no role mapped, through at its role and above. Through means past the gate:
     /// the node doesn't run its generation here, so most routes answer 503 themselves.
@@ -1057,7 +1092,7 @@ mod tests {
         let idp = Issuer::start().await;
         let addr = serve(api(Arc::new(Auth::new(
             &idp.api("roles"),
-            crate::test_support::USERNAME_CLAIM,
+            &crate::test_support::console(),
         ))))
         .await;
         let callers = [
````

`crates/data-avalanche-api/src/nodes.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/nodes.rs b/crates/data-avalanche-api/src/nodes.rs
index e868aa4..ec6b5cf 100644
--- a/crates/data-avalanche-api/src/nodes.rs
+++ b/crates/data-avalanche-api/src/nodes.rs
@@ -454,8 +454,8 @@ mod tests {
     use super::*;
     use crate::peers::{ANSWERED_BY, Peers, TARGET};
     use crate::test_support::{
-        CONFIG, Issuer, Log, Seen, USERNAME_CLAIM, api, fake, lone_with, member, open, request,
-        serve, served,
+        CONFIG, Issuer, Log, Seen, api, console, fake, lone_with, member, open, request, serve,
+        served,
     };
     use crate::{Auth, Stop};
 
@@ -483,6 +483,8 @@ mod tests {
                     authorization: headers
                         .get("authorization")
                         .map(|v| v.to_str().unwrap().to_owned()),
+                    cookie: None,
+                    csrf: None,
                     target: None,
                     forwarded_by: None,
                     body: String::new(),
@@ -749,6 +751,37 @@ mod tests {
         assert_eq!(seen.forwarded_by.as_deref(), Some("node-a"));
     }
 
+    /// M13f's carry-in: a peer opens the console's session with the key every node has, so a
+    /// node passes the session on as it passes a token, with its anti-forgery token.
+    #[tokio::test]
+    async fn a_node_action_is_forwarded_with_the_callers_session() {
+        let (b, log) = fake(Duration::ZERO).await;
+        let auth = Auth::new(
+            &crate::test_support::basic_api("s3cret"),
+            &crate::test_support::sessions(Duration::from_secs(3600)),
+        );
+        let (addr, _lone, _dir) = served(&b, Arc::new(auth)).await;
+        let session = crate::test_support::basic_session("s3cret", Duration::from_secs(60));
+        let cookie = format!(
+            "theme=dark; {}",
+            crate::test_support::session_cookie(&session)
+        );
+        let sent = [
+            ("Cookie", cookie.as_str()),
+            (crate::CSRF_HEADER, session.csrf.as_str()),
+        ];
+        let (code, _, text) = request(addr, "POST", "/v1/nodes/node-b/leave", &sent, "").await;
+        assert_eq!(code, 202, "{text}");
+        let seen = log.lock().unwrap()[0].clone();
+        assert_eq!(seen.authorization, None);
+        assert_eq!(
+            seen.cookie.as_deref(),
+            cookie.strip_prefix("theme=dark; "),
+            "the session's cookies alone"
+        );
+        assert_eq!(seen.csrf.as_deref(), Some(session.csrf.as_str()));
+    }
+
     #[tokio::test]
     async fn a_peer_that_doesnt_answer_is_named_within_the_limit() {
         let (addr, _lone, _dir) = served("http://127.0.0.1:1", open()).await;
@@ -853,7 +886,7 @@ mod tests {
         let idp = Issuer::start().await;
         let (b, log) = fake(Duration::ZERO).await;
         let (addr, lone, _dir) =
-            served(&b, Arc::new(Auth::new(&idp.api("roles"), USERNAME_CLAIM))).await;
+            served(&b, Arc::new(Auth::new(&idp.api("roles"), &console()))).await;
         let token = format!("Bearer {}", idp.token(json!(["viewer"])));
         let auth = [("Authorization", token.as_str())];
         let (code, _, _) = request(addr, "POST", "/v1/nodes/node-b/leave", &auth, "").await;
````

`crates/data-avalanche-api/src/session.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/session.rs b/crates/data-avalanche-api/src/session.rs
new file mode 100644
index 0000000..b971e32
--- /dev/null
+++ b/crates/data-avalanche-api/src/session.rs
@@ -0,0 +1,163 @@
+#[cfg(test)]
+mod tests {
+    use axum::http::HeaderValue;
+
+    use super::*;
+
+    const KEY: &str = "a session key of at least thirty-two bytes";
+
+    fn basic() -> Session {
+        Session::new(
+            By::Basic {
+                user: "ops".into(),
+                fingerprint: Sealer::new(KEY).fingerprint("s3cret"),
+            },
+            1_700_000_000,
+        )
+    }
+
+    /// The `Cookie` header a browser sends back for these `Set-Cookie` values.
+    fn sent_back(set: &[String]) -> HeaderMap {
+        let pairs: Vec<&str> = set
+            .iter()
+            .filter(|c| !c.contains("Max-Age=0"))
+            .map(|c| c.split(';').next().unwrap())
+            .collect();
+        let mut h = HeaderMap::new();
+        h.insert(COOKIE, HeaderValue::from_str(&pairs.join("; ")).unwrap());
+        h
+    }
+
+    #[test]
+    fn a_session_sealed_by_one_node_opens_on_another_with_the_same_key() {
+        let s = basic();
+        let set = s.cookies(&Sealer::new(KEY), 3600).unwrap();
+        assert_eq!(
+            set[0].split_once('=').unwrap().0,
+            "da_session",
+            "the first cookie"
+        );
+        assert!(
+            set[0].ends_with("; Path=/; Max-Age=3600; HttpOnly; Secure; SameSite=Strict"),
+            "{}",
+            set[0]
+        );
+        let other = Sealer::new(KEY);
+        assert_eq!(Session::of(&sent_back(&set), &other), Some(Ok(s)));
+    }
+
+    #[test]
+    fn a_session_key_changed_is_told_from_a_cookie_altered() {
+        let set = basic().cookies(&Sealer::new(KEY), 3600).unwrap();
+        let other = Sealer::new("another session key of thirty-two bytes");
+        assert_eq!(
+            Session::of(&sent_back(&set), &other),
+            Some(Err(Unsealed::KeyChanged))
+        );
+        // One character of the ciphertext changed.
+        let mut altered = set[0].clone();
+        let at = altered.find(';').unwrap() - 3;
+        let flipped = if &altered[at..=at] == "A" { "B" } else { "A" };
+        altered.replace_range(at..=at, flipped);
+        assert_eq!(
+            Session::of(&sent_back(&[altered]), &Sealer::new(KEY)),
+            Some(Err(Unsealed::Garbled))
+        );
+        assert_eq!(Session::of(&HeaderMap::new(), &Sealer::new(KEY)), None);
+    }
+
+    #[test]
+    fn a_value_sealed_for_one_purpose_doesnt_open_as_another() {
+        let sealer = Sealer::new(KEY);
+        let sealed = sealer.seal("oauth2", b"{}");
+        assert_eq!(sealer.open("oauth2", &sealed).unwrap(), b"{}");
+        assert_eq!(sealer.open("session", &sealed), Err(Unsealed::Garbled));
+    }
+
+    #[test]
+    fn a_long_session_takes_several_cookies_and_too_long_a_one_none() {
+        let sealer = Sealer::new(KEY);
+        let token = |n: usize| "t".repeat(n);
+        let s = Session::new(
+            By::Oauth2 {
+                access: token(3000),
+                refresh: Some(token(1500)),
+            },
+            1_700_000_000,
+        );
+        let set = s.cookies(&sealer, 60).unwrap();
+        let names: Vec<&str> = set.iter().map(|c| c.split('=').next().unwrap()).collect();
+        assert_eq!(
+            names,
+            ["da_session", "da_session.1", "da_session.2", "da_session.3"]
+        );
+        assert!(
+            set[1].len() < 4096 && set[2].contains("Max-Age=0"),
+            "{set:?}"
+        );
+        assert!(
+            set[3].contains("Max-Age=0"),
+            "an older session's cookie removed"
+        );
+        let back = sent_back(&set);
+        assert_eq!(Session::of(&back, &sealer), Some(Ok(s)));
+        assert_eq!(
+            session_cookies(&back)
+                .unwrap()
+                .matches("da_session")
+                .count(),
+            2
+        );
+        let huge = Session::new(
+            By::Oauth2 {
+                access: token(16_000),
+                refresh: None,
+            },
+            1_700_000_000,
+        );
+        assert!(matches!(huge.cookies(&sealer, 60), Err(TooLong(_))));
+    }
+
+    #[test]
+    fn the_session_cookies_alone_are_passed_on() {
+        let mut h = HeaderMap::new();
+        h.insert(
+            COOKIE,
+            HeaderValue::from_static("theme=dark; da_session=abc; other=x"),
+        );
+        h.append(COOKIE, HeaderValue::from_static("da_session.1=def"));
+        assert_eq!(
+            session_cookies(&h).as_deref(),
+            Some("da_session=abc; da_session.1=def")
+        );
+        assert_eq!(sealed_value(&h).as_deref(), Some("abcdef"));
+    }
+
+    #[test]
+    fn a_password_s_fingerprint_depends_on_the_key_and_the_password() {
+        let a = Sealer::new(KEY);
+        assert_eq!(
+            a.fingerprint("s3cret"),
+            Sealer::new(KEY).fingerprint("s3cret")
+        );
+        assert_ne!(a.fingerprint("s3cret"), a.fingerprint("s3cret2"));
+        let b = Sealer::new("another session key of thirty-two bytes");
+        assert_ne!(a.fingerprint("s3cret"), b.fingerprint("s3cret"));
+    }
+
+    #[test]
+    fn a_session_s_tokens_never_print() {
+        let mut s = Session::new(
+            By::Oauth2 {
+                access: "the-access-token".into(),
+                refresh: Some("the-refresh-token".into()),
+            },
+            0,
+        );
+        s.csrf = "the-csrf-token".into();
+        let printed = format!("{s:?}");
+        for secret in ["the-access-token", "the-refresh-token", "the-csrf-token"] {
+            assert!(!printed.contains(secret), "{printed}");
+        }
+    }
+}
````

`crates/data-avalanche-api/src/stream.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/stream.rs b/crates/data-avalanche-api/src/stream.rs
index 9c424e2..27264ec 100644
--- a/crates/data-avalanche-api/src/stream.rs
+++ b/crates/data-avalanche-api/src/stream.rs
@@ -830,7 +830,7 @@ mod tests {
         .await;
         let auth = std::sync::Arc::new(crate::Auth::new(
             &basic("s3cret"),
-            crate::test_support::USERNAME_CLAIM,
+            &crate::test_support::console(),
         ));
         let (addr, _lone, _dir) = served_with(&b_url, std::sync::Arc::clone(&auth), |_| {}).await;
         let mut s = stream(addr, "/v1/stream", &[("authorization", OPS)]).await;
@@ -838,7 +838,7 @@ mod tests {
         s.next().await;
         assert_eq!(s.next().await.1["state"], "unreachable");
         // The password changes before node-b is followed again (FOLLOW_BACKOFF[0]).
-        auth.set(&basic("rotated"), crate::test_support::USERNAME_CLAIM);
+        auth.set(&basic("rotated"), &crate::test_support::console());
         assert_eq!(
             s.next().await,
             (
@@ -862,7 +862,7 @@ mod tests {
         .await;
         let auth = std::sync::Arc::new(crate::Auth::new(
             &basic("s3cret"),
-            crate::test_support::USERNAME_CLAIM,
+            &crate::test_support::console(),
         ));
         let (addr, lone, _dir) = served_with(&b_url, std::sync::Arc::clone(&auth), |_| {}).await;
         let mut s = stream(addr, "/v1/stream", &[("authorization", OPS)]).await;
@@ -877,7 +877,7 @@ mod tests {
         // `auth: basic` without its credentials: every caller is answered 503, none refused.
         let mut refused = basic("s3cret");
         refused.credentials = None;
-        auth.set(&refused, crate::test_support::USERNAME_CLAIM);
+        auth.set(&refused, &crate::test_support::console());
         assert_eq!(
             s.next().await,
             ("peer".into(), unreachable),
@@ -898,7 +898,7 @@ mod tests {
         .await;
         let auth = std::sync::Arc::new(crate::Auth::new(
             &basic("s3cret"),
-            crate::test_support::USERNAME_CLAIM,
+            &crate::test_support::console(),
         ));
         let (addr, lone, _dir) = served_with(&b_url, auth, |_| {}).await;
         let mut s = stream(addr, "/v1/stream", &[("authorization", OPS)]).await;
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index 3d7e37e..6e203f0 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -22,7 +22,8 @@ use data_avalanche_cluster::{
     Phase, ReconcilerSettings, Recorder, Udp, WorkerFactory, file_hash, removed_pipelines,
 };
 use data_avalanche_config::{
-    ApiAuth, Env, FingerprintKey, HostPort, Jwt, Pipeline, ProcessEnv, ResolvedConfig, load_str,
+    ApiAuth, Console, Env, FingerprintKey, HostPort, Jwt, Pipeline, ProcessEnv, ResolvedConfig,
+    Secret, load_str,
 };
 use data_avalanche_pipeline::{Backoff, Tuning};
 use data_avalanche_state::{
@@ -72,12 +73,86 @@ pub(crate) fn config() -> ResolvedConfig {
         .config
 }
 
-/// `globals.console.username_claim`'s default.
-pub(crate) const USERNAME_CLAIM: &str = "preferred_username";
+/// `globals.console` of [`CONFIG`]: disabled, its defaults.
+pub(crate) fn console() -> Console {
+    config().globals.console
+}
+
+/// [`console`], naming a token's caller by `username_claim`.
+pub(crate) fn console_naming(username_claim: &str) -> Console {
+    Console {
+        username_claim: username_claim.into(),
+        ..console()
+    }
+}
+
+/// The session key the tests' consoles seal with.
+pub(crate) const SESSION_KEY: &str = "the tests' session key, of 32 bytes or more";
+
+/// [`console`], enabled, its sessions sealed with [`SESSION_KEY`] and lasting `ttl`.
+pub(crate) fn sessions(ttl: Duration) -> Console {
+    Console {
+        enabled: true,
+        session_key: Some(Secret::new(
+            SESSION_KEY,
+            &FingerprintKey::from_bytes([1; 32]),
+        )),
+        session_ttl: ttl,
+        ..console()
+    }
+}
+
+/// `globals.api` with `auth: basic`, its one user `ops` with password `password`.
+pub(crate) fn basic_api(password: &str) -> data_avalanche_config::Api {
+    let mut api = config().globals.api;
+    api.auth = ApiAuth::Basic;
+    api.credentials = Some(data_avalanche_config::Credentials::Inline {
+        user: "ops".into(),
+        password: Secret::new(password, &FingerprintKey::from_bytes([1; 32])),
+    });
+    api
+}
+
+/// The `Cookie` header that carries `session`, sealed with [`SESSION_KEY`].
+pub(crate) fn session_cookie(session: &crate::session::Session) -> String {
+    let sealer = crate::session::Sealer::new(SESSION_KEY);
+    session
+        .cookies(&sealer, 3600)
+        .unwrap()
+        .iter()
+        .filter(|c| !c.contains("Max-Age=0"))
+        .map(|c| c.split(';').next().unwrap().to_owned())
+        .collect::<Vec<_>>()
+        .join("; ")
+}
+
+/// A basic session of user `ops`, who gave `password`, signed in `ago` before now.
+pub(crate) fn basic_session(password: &str, ago: Duration) -> crate::session::Session {
+    let sealer = crate::session::Sealer::new(SESSION_KEY);
+    crate::session::Session::new(
+        crate::session::By::Basic {
+            user: "ops".into(),
+            fingerprint: sealer.fingerprint(password),
+        },
+        chrono::Utc::now().timestamp() - i64::try_from(ago.as_secs()).unwrap(),
+    )
+}
+
+/// What a request with these headers presented.
+pub(crate) fn presented(headers: &[(&str, &str)]) -> crate::auth::Presented {
+    let mut h = HeaderMap::new();
+    for (k, v) in headers {
+        h.append(
+            axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
+            v.parse().unwrap(),
+        );
+    }
+    crate::auth::Presented::of(&h)
+}
 
 /// `globals.api` of [`CONFIG`]: `auth: none`.
 pub(crate) fn open() -> Arc<Auth> {
-    Arc::new(Auth::new(&config().globals.api, USERNAME_CLAIM))
+    Arc::new(Auth::new(&config().globals.api, &console()))
 }
 
 /// The routes' state before the node runs its generation: the `/v1` routes answer 503.
@@ -441,6 +516,25 @@ impl Issuer {
         }
     }
 
+    /// `globals.api` with `auth: oauth2` for this issuer, its keys at its JWKS, the console's
+    /// client `cdc-console`.
+    pub fn oauth2_api(&self, roles_claim: &str) -> data_avalanche_config::Api {
+        let mut api = config().globals.api;
+        api.auth = ApiAuth::Oauth2;
+        api.oauth2 = Some(data_avalanche_config::ApiOauth2 {
+            issuer: self.url.clone(),
+            audience: Some("cdc".into()),
+            jwks_url: Some(format!("{}/jwks", self.url)),
+            discovery_url: None,
+            roles_claim: roles_claim.into(),
+            client_id: Some("cdc-console".into()),
+            client_secret: None,
+            scopes: vec!["openid".into(), "profile".into()],
+            idp_name: None,
+        });
+        api
+    }
+
     /// `globals.api` with `auth: jwt` for this issuer.
     pub fn api(&self, roles_claim: &str) -> data_avalanche_config::Api {
         let mut api = config().globals.api;
@@ -489,6 +583,8 @@ pub(crate) struct Seen {
     pub method: Method,
     pub uri: String,
     pub authorization: Option<String>,
+    pub cookie: Option<String>,
+    pub csrf: Option<String>,
     pub target: Option<String>,
     pub forwarded_by: Option<String>,
     pub body: String,
@@ -519,6 +615,8 @@ pub(crate) async fn fake_answering(
                     method,
                     uri: uri.to_string(),
                     authorization: h("authorization"),
+                    cookie: h("cookie"),
+                    csrf: h(crate::CSRF_HEADER),
                     target: h(TARGET),
                     forwarded_by: h(FORWARDED_BY),
                     body: String::from_utf8(bytes.to_vec()).unwrap(),
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api --lib`
Expected: the API's tests don't compile: they find no `Sealer`, `Session`, `By`, `Unsealed`, `TooLong`, `session_cookies` or `sealed_value` (the new module holds only its tests), no `Presented`, `CSRF_HEADER`, `Principal::expires` or `Auth::verify_caller` (nor the imports the code above them brings: `DateTime`, `Utc`, `HeaderMap`); `Auth::new` and `Auth::set` take the username claim (`&str`), not the `Console`.

- [ ] **Step 3: Implement**

`session.rs` seals with `aws_lc_rs::aead` (AES-256-GCM, `LessSafeKey`), its key and its key id
from `blake3::derive_key` of `session_key`. `Auth` keeps the `Sessions` of its generation and
swaps them with the rest on a reload. The gate (`authorize`) checks the anti-forgery token before
the role. `aws-lc-rs` becomes a workspace dependency, without its default features.

`Cargo.toml`:

````diff
diff --git a/Cargo.toml b/Cargo.toml
index 51da7c5..dfa3d7b 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -95,6 +95,9 @@ jsonwebtoken = { version = "=12.0.0", default-features = false, features = ["aws
 reqwest = { version = "0.13.5", default-features = false, features = ["rustls"] }
 base64 = "0.23.1"
 subtle = "2.6.1"
+# The console's session cookie (M14): AES-256-GCM and the random source of the aws-lc-rs rustls
+# and jsonwebtoken already build on.
+aws-lc-rs = { version = "1.19", default-features = false, features = ["aws-lc-sys"] }
 # Its OpenAPI document (roadmap §4 M9), built from the route table with the builders. 6.0.0
 # doesn't build without `macros`: its schema module names a type only that feature defines.
 utoipa = { version = "=6.0.0", default-features = false, features = ["macros"] }
````

`crates/data-avalanche-api/Cargo.toml`:

````diff
diff --git a/crates/data-avalanche-api/Cargo.toml b/crates/data-avalanche-api/Cargo.toml
index e5675e6..d34f33f 100644
--- a/crates/data-avalanche-api/Cargo.toml
+++ b/crates/data-avalanche-api/Cargo.toml
@@ -11,6 +11,7 @@ workspace = true
 
 [dependencies]
 arrow-schema.workspace = true
+aws-lc-rs.workspace = true
 axum.workspace = true
 base64.workspace = true
 blake3.workspace = true
````

`crates/data-avalanche-api/src/auth.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/auth.rs b/crates/data-avalanche-api/src/auth.rs
index f9af2cd..eff3711 100644
--- a/crates/data-avalanche-api/src/auth.rs
+++ b/crates/data-avalanche-api/src/auth.rs
@@ -6,22 +6,31 @@
 //! never introspected (no client credentials are configured). A token's caller is named by
 //! `globals.console.username_claim`, else its `sub`. The settings are R: [`Auth::set`] takes a
 //! reload's. Settings without what their mode reads refuse every request (503).
+//!
+//! With the console enabled and `auth` `basic` or `oauth2`, a request without `Authorization` may
+//! carry the console's session cookie instead (Admin_Console §9, [`crate::session`]): every node
+//! opens it with the session key, and verifies its person again as it verifies credentials, on
+//! every request. A session ends `session_ttl` after sign-in, when the basic user or password
+//! changes, when its provider's token no longer verifies, or when the key or the mode changes.
 
 use std::collections::BTreeMap;
 use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
 use std::time::{Duration, Instant};
 
-use axum::http::HeaderMap;
-use axum::http::header::AUTHORIZATION;
+use axum::http::header::{AUTHORIZATION, COOKIE};
+use axum::http::{HeaderMap, HeaderValue};
 use base64::Engine;
 use base64::engine::general_purpose::STANDARD;
-use data_avalanche_config::{Api as ApiConfig, ApiAuth, Credentials, Role};
+use chrono::{DateTime, SecondsFormat, Utc};
+use data_avalanche_config::{Api as ApiConfig, ApiAuth, Console, Credentials, Role, Secret};
 use jsonwebtoken::jwk::JwkSet;
 use jsonwebtoken::{AlgorithmFamily, DecodingKey, Validation, decode, decode_header};
 use serde_json::Value;
 use subtle::ConstantTimeEq;
 use tokio::sync::watch;
 
+use crate::session::{By, Sealer, Session, Unsealed, session_cookies};
+
 /// A JWKS fetch, or a discovery, takes at most this long.
 pub const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
 
@@ -46,12 +55,72 @@ struct Timing {
     max_age: Duration,
 }
 
+/// The header a mutating request the console's session authenticates carries its anti-forgery
+/// token in (Admin_Console §9).
+pub const CSRF_HEADER: &str = "x-csrf-token";
+
 /// An authenticated caller.
 #[derive(Debug, Clone, PartialEq, Eq)]
 pub struct Principal {
     /// The basic user, the token's `username_claim` (else its `sub`), or `anonymous`.
     pub name: String,
     pub role: Role,
+    /// When its credentials stop verifying: a token's `exp`, a session's end (the earlier of
+    /// the two for an `oauth2` session). `None`: they don't expire (basic credentials, `none`).
+    pub expires: Option<DateTime<Utc>>,
+}
+
+/// A caller verified, with the session that verified them, if one did.
+#[derive(Debug, Clone)]
+pub(crate) struct Verified {
+    pub principal: Principal,
+    pub session: Option<Session>,
+}
+
+/// The credentials a caller presented, passed on to a peer as they came (Admin_Console §10): its
+/// `Authorization`, its session's cookies and its anti-forgery token. They never print.
+#[derive(Clone, Default)]
+pub(crate) struct Presented {
+    authorization: Option<HeaderValue>,
+    cookie: Option<HeaderValue>,
+    csrf: Option<HeaderValue>,
+}
+
+impl std::fmt::Debug for Presented {
+    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
+        let shown = |v: &Option<HeaderValue>| v.as_ref().map(|_| "<redacted>");
+        f.debug_struct("Presented")
+            .field("authorization", &shown(&self.authorization))
+            .field("cookie", &shown(&self.cookie))
+            .field("csrf", &shown(&self.csrf))
+            .finish()
+    }
+}
+
+impl Presented {
+    /// What a request with these headers presented: of its cookies, the session's alone.
+    pub(crate) fn of(headers: &HeaderMap) -> Self {
+        Self {
+            authorization: headers.get(AUTHORIZATION).cloned(),
+            cookie: session_cookies(headers).and_then(|c| HeaderValue::from_str(&c).ok()),
+            csrf: headers.get(CSRF_HEADER).cloned(),
+        }
+    }
+
+    /// As headers of a request to a peer, or of the check [`Auth::verify`] makes again.
+    pub(crate) fn headers(&self) -> HeaderMap {
+        let mut h = HeaderMap::new();
+        for (name, value) in [
+            (AUTHORIZATION.as_str(), &self.authorization),
+            (COOKIE.as_str(), &self.cookie),
+            (CSRF_HEADER, &self.csrf),
+        ] {
+            if let Some(v) = value {
+                h.insert(name, v.clone());
+            }
+        }
+        h
+    }
 }
 
 /// Why a caller is turned away.
@@ -82,21 +151,53 @@ struct Settings {
     role_mapping: BTreeMap<Role, Vec<String>>,
     /// The token claim that names the caller (`globals.console.username_claim`).
     username_claim: String,
+    /// The console's session key and `session_ttl`: set when the console is enabled with `auth`
+    /// `basic` or `oauth2` and a key (V-135).
+    sessions: Option<(Secret, Duration)>,
 }
 
 impl Settings {
-    fn of(api: &ApiConfig, username_claim: &str) -> Self {
+    fn of(api: &ApiConfig, console: &Console) -> Self {
+        let sessions = (console.enabled && matches!(api.auth, ApiAuth::Basic | ApiAuth::Oauth2))
+            .then(|| console.session_key.clone())
+            .flatten()
+            .map(|key| (key, console.session_ttl));
         Self {
             auth: api.auth,
             credentials: api.credentials.clone(),
             jwt: api.jwt.clone(),
             oauth2: api.oauth2.clone(),
             role_mapping: api.role_mapping.clone(),
-            username_claim: username_claim.to_owned(),
+            username_claim: console.username_claim.clone(),
+            sessions,
         }
     }
 }
 
+/// The console's sessions: the sealer of the session key, and how long a session lasts.
+#[derive(Debug)]
+pub(crate) struct Sessions {
+    pub(crate) sealer: Sealer,
+    pub(crate) ttl: Duration,
+}
+
+impl Sessions {
+    fn of(s: &Settings) -> Option<Arc<Self>> {
+        s.sessions.as_ref().map(|(key, ttl)| {
+            Arc::new(Self {
+                sealer: Sealer::new(key.expose()),
+                ttl: *ttl,
+            })
+        })
+    }
+
+    /// When a session signed in at `signed_in` (Unix seconds) ends.
+    pub(crate) fn ends(&self, signed_in: i64) -> DateTime<Utc> {
+        DateTime::from_timestamp(signed_in, 0).unwrap_or_default()
+            + chrono::Duration::from_std(self.ttl).unwrap_or_default()
+    }
+}
+
 #[derive(Debug)]
 enum Mode {
     Open,
@@ -110,6 +211,7 @@ enum Mode {
 struct Current {
     settings: Settings,
     mode: Arc<Mode>,
+    sessions: Option<Arc<Sessions>>,
 }
 
 /// The node's authentication, swapped by a reload.
@@ -120,17 +222,17 @@ pub struct Auth {
 }
 
 impl Auth {
-    /// The authentication of `globals.api`, a token's caller named by its `username_claim`
-    /// (`globals.console.username_claim`), else its `sub`.
-    pub fn new(api: &ApiConfig, username_claim: &str) -> Self {
-        Self::build(api, username_claim, REFETCH_AFTER, KEYS_MAX_AGE)
+    /// The authentication of `globals.api`, a token's caller named by `console`'s
+    /// `username_claim`, else its `sub`; with the console's sessions when `console` enables them.
+    pub fn new(api: &ApiConfig, console: &Console) -> Self {
+        Self::build(api, console, REFETCH_AFTER, KEYS_MAX_AGE)
     }
 
     #[cfg(test)]
     pub(crate) fn with_refetch_after(api: &ApiConfig, refetch_after: Duration) -> Self {
         Self::build(
             api,
-            crate::test_support::USERNAME_CLAIM,
+            &crate::test_support::console(),
             refetch_after,
             KEYS_MAX_AGE,
         )
@@ -138,17 +240,12 @@ impl Auth {
 
     #[cfg(test)]
     pub(crate) fn with_timing(api: &ApiConfig, refetch_after: Duration, max_age: Duration) -> Self {
-        Self::build(
-            api,
-            crate::test_support::USERNAME_CLAIM,
-            refetch_after,
-            max_age,
-        )
+        Self::build(api, &crate::test_support::console(), refetch_after, max_age)
     }
 
     fn build(
         api: &ApiConfig,
-        username_claim: &str,
+        console: &Console,
         refetch_after: Duration,
         max_age: Duration,
     ) -> Self {
@@ -156,10 +253,11 @@ impl Auth {
             refetch_after,
             max_age,
         };
-        let settings = Settings::of(api, username_claim);
+        let settings = Settings::of(api, console);
         Self {
             current: RwLock::new(Current {
                 mode: Arc::new(mode(&settings, timing, None)),
+                sessions: Sessions::of(&settings),
                 settings,
             }),
             timing,
@@ -168,10 +266,10 @@ impl Auth {
 
     /// Takes a reload's settings. Unchanged ones keep the cached JWKS, and so do changed ones
     /// whose keys come from where they did (the same issuer and JWKS or discovery URL): a
-    /// change of the audience, the roles claim, the role mapping or the username claim fetches
-    /// nothing.
-    pub fn set(&self, api: &ApiConfig, username_claim: &str) {
-        let settings = Settings::of(api, username_claim);
+    /// change of the audience, the roles claim, the role mapping, the username claim or the
+    /// console's sessions fetches nothing.
+    pub fn set(&self, api: &ApiConfig, console: &Console) {
+        let settings = Settings::of(api, console);
         let mut current = self.current.write().unwrap_or_else(PoisonError::into_inner);
         if current.settings != settings {
             let keys = match &*current.mode {
@@ -179,28 +277,43 @@ impl Auth {
                 _ => None,
             };
             current.mode = Arc::new(mode(&settings, self.timing, keys));
+            current.sessions = Sessions::of(&settings);
             current.settings = settings;
         }
     }
 
     /// The caller of a request with these headers.
     pub(crate) async fn verify(&self, headers: &HeaderMap) -> Result<Principal, Denied> {
-        let mode = Arc::clone(
-            &self
-                .current
-                .read()
-                .unwrap_or_else(PoisonError::into_inner)
-                .mode,
-        );
-        match &*mode {
+        self.verify_caller(headers).await.map(|v| v.principal)
+    }
+
+    /// The caller of a request with these headers, and their session: one is opened when the
+    /// request has no `Authorization` and the console's sessions are on.
+    pub(crate) async fn verify_caller(&self, headers: &HeaderMap) -> Result<Verified, Denied> {
+        let (mode, sessions) = {
+            let current = self.current.read().unwrap_or_else(PoisonError::into_inner);
+            (Arc::clone(&current.mode), current.sessions.clone())
+        };
+        if !headers.contains_key(AUTHORIZATION)
+            && let Some(sessions) = &sessions
+            && let Some(opened) = Session::of(headers, &sessions.sealer)
+        {
+            return session(&mode, sessions, opened, Utc::now()).await;
+        }
+        let principal = match &*mode {
             Mode::Open => Ok(Principal {
                 name: "anonymous".into(),
                 role: Role::Admin,
+                expires: None,
             }),
             Mode::Basic(credentials) => basic(credentials, headers),
             Mode::Token(verifier) => verifier.verify(headers).await,
             Mode::Refused => Err(Denied::Unavailable(MISCONFIGURED.into())),
-        }
+        }?;
+        Ok(Verified {
+            principal,
+            session: None,
+        })
     }
 
     /// Waits for the JWKS fetch under way, if any.
@@ -267,6 +380,78 @@ fn mode(s: &Settings, timing: Timing, keys: Option<Arc<Keys>>) -> Mode {
     }
 }
 
+/// A session's person, verified again at `now`: it ends `ttl` after sign-in, whatever its use;
+/// a basic session's user and password are still the configured ones; an oauth2 session's
+/// access token verifies as a bearer token does.
+async fn session(
+    mode: &Mode,
+    sessions: &Sessions,
+    opened: Result<Session, Unsealed>,
+    now: DateTime<Utc>,
+) -> Result<Verified, Denied> {
+    let challenge = match mode {
+        Mode::Basic(_) => BASIC,
+        _ => BEARER,
+    };
+    let session = opened.map_err(|e| unauthenticated(challenge, e.to_string()))?;
+    let ends = sessions.ends(session.signed_in);
+    if now >= ends {
+        return Err(unauthenticated(
+            challenge,
+            format!(
+                "the session expired at {} ({} after sign-in)",
+                ends.to_rfc3339_opts(SecondsFormat::Secs, true),
+                span(sessions.ttl)
+            ),
+        ));
+    }
+    let principal = match (mode, &session.by) {
+        (Mode::Basic(c), By::Basic { user, fingerprint }) => {
+            let now_password = sessions.sealer.fingerprint(c.password().expose());
+            if !bool::from(same(user, c.user()) & same(fingerprint, &now_password)) {
+                return Err(unauthenticated(
+                    BASIC,
+                    "the user or the password changed since the sign-in",
+                ));
+            }
+            Principal {
+                name: user.clone(),
+                role: Role::Admin,
+                expires: Some(ends),
+            }
+        }
+        (Mode::Token(verifier), By::Oauth2 { access, .. }) => {
+            let mut p = verifier.verify_token(access).await.map_err(|d| match d {
+                Denied::Unauthenticated { challenge, why } => {
+                    unauthenticated(challenge, format!("the provider's token: {why}"))
+                }
+                other => other,
+            })?;
+            p.expires = Some(p.expires.map_or(ends, |at| at.min(ends)));
+            p
+        }
+        _ => {
+            return Err(unauthenticated(
+                challenge,
+                "the cluster's sign-in changed since the session began",
+            ));
+        }
+    };
+    Ok(Verified {
+        principal,
+        session: Some(session),
+    })
+}
+
+/// A duration as a page says it: `8 h`, `90 min`, `45 s`.
+pub(crate) fn span(d: Duration) -> String {
+    match d.as_secs() {
+        s if s % 3600 == 0 => format!("{} h", s / 3600),
+        s if s % 60 == 0 => format!("{} min", s / 60),
+        s => format!("{s} s"),
+    }
+}
+
 fn unauthenticated(challenge: &'static str, why: impl Into<String>) -> Denied {
     Denied::Unauthenticated {
         challenge,
@@ -306,6 +491,7 @@ fn basic(c: &Credentials, headers: &HeaderMap) -> Result<Principal, Denied> {
         Ok(Principal {
             name: user.to_owned(),
             role: Role::Admin,
+            expires: None,
         })
     } else {
         Err(unauthenticated(BASIC, "wrong user or password"))
@@ -326,6 +512,11 @@ impl Verifier {
     async fn verify(&self, headers: &HeaderMap) -> Result<Principal, Denied> {
         let token = credential(headers, "Bearer")
             .ok_or_else(|| unauthenticated(BEARER, "a bearer token is required"))?;
+        self.verify_token(token).await
+    }
+
+    /// The caller `token` names, once it verifies.
+    pub(crate) async fn verify_token(&self, token: &str) -> Result<Principal, Denied> {
         let header = decode_header(token)
             .map_err(|e| unauthenticated(BEARER, format!("not a token: {e}")))?;
         if AlgorithmFamily::Hmac == header.alg.family() {
@@ -362,6 +553,10 @@ impl Verifier {
             .or_else(|| claims.get("sub").and_then(Value::as_str))
             .unwrap_or("unknown")
             .to_owned();
+        let expires = claims
+            .get("exp")
+            .and_then(Value::as_i64)
+            .and_then(|exp| DateTime::from_timestamp(exp, 0));
         let Some(role) = roles(&claims, &self.roles_claim)
             .iter()
             .filter_map(|r| map(&self.mapping, r))
@@ -375,7 +570,11 @@ impl Verifier {
                 name,
             });
         };
-        Ok(Principal { name, role })
+        Ok(Principal {
+            name,
+            role,
+            expires,
+        })
     }
 }
 
````

`crates/data-avalanche-api/src/follow.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/follow.rs b/crates/data-avalanche-api/src/follow.rs
index 4e2059e..8f0688f 100644
--- a/crates/data-avalanche-api/src/follow.rs
+++ b/crates/data-avalanche-api/src/follow.rs
@@ -5,8 +5,9 @@
 
 use std::time::Duration;
 
-use axum::http::{HeaderValue, header};
+use axum::http::header;
 
+use crate::auth::Presented;
 use crate::peers::{DETAIL_LIMIT, FORWARDED_BY, Peers, TARGET};
 
 /// The largest message read from a followed stream.
@@ -137,8 +138,8 @@ async fn reason(mut answer: reqwest::Response) -> Option<String> {
 }
 
 impl Peers {
-    /// Opens node `node`'s stream at `api_url` + `path_and_query` with the caller's
-    /// `authorization`, answered within [`DETAIL_LIMIT`]; dropped once it sends nothing for
+    /// Opens node `node`'s stream at `api_url` + `path_and_query` with the credentials the
+    /// caller `presented`, answered within [`DETAIL_LIMIT`]; dropped once it sends nothing for
     /// `silence`. An answer other than `200` is an error naming its code and its own reason, read
     /// to a cap and clipped.
     pub(crate) async fn follow(
@@ -146,18 +147,16 @@ impl Peers {
         node: &str,
         api_url: &str,
         path_and_query: &str,
-        authorization: Option<HeaderValue>,
+        presented: &Presented,
         silence: Duration,
     ) -> Result<Followed, String> {
-        let mut req = self
+        let req = self
             .http
             .get(format!("{api_url}{path_and_query}"))
+            .headers(presented.headers())
             .header(TARGET, node)
             .header(FORWARDED_BY, &self.me)
             .header(header::ACCEPT, "text/event-stream");
-        if let Some(a) = authorization {
-            req = req.header(header::AUTHORIZATION, a);
-        }
         let answer = match tokio::time::timeout(DETAIL_LIMIT, req.send()).await {
             Err(_) => return Err(format!("it didn't answer within {DETAIL_LIMIT:?}")),
             Ok(Err(e)) => return Err(crate::peers::cause(&e)),
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index c4247e0..3dfe4aa 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -6,7 +6,9 @@
 //!
 //! Every `/v1` route takes the least role in API_RBAC §2 ([`routes`]): no credentials, or ones
 //! that don't verify, are 401 with a `WWW-Authenticate` challenge; a caller below the route's
-//! role, or with no role mapped, is 403, and recorded on a route that changes something.
+//! role, or with no role mapped, is 403, and recorded on a route that changes something. A
+//! request the console's session authenticates sends the session's anti-forgery token
+//! ([`CSRF_HEADER`]) when it changes something: without it, 403, before its role is looked at.
 
 mod actions;
 mod auth;
@@ -39,7 +41,7 @@ use std::sync::{Arc, OnceLock};
 
 use axum::Router;
 use axum::extract::{Request, State};
-use axum::http::{HeaderValue, StatusCode, header};
+use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
 use axum::middleware::{Next, from_fn_with_state};
 use axum::response::{IntoResponse, Response};
 use axum::routing::{MethodRouter, get, post};
@@ -54,7 +56,9 @@ use serde_json::{Value, json};
 use tokio::sync::watch;
 
 pub use actions::{DENIALS_BURST, MAX_ERROR, MAX_NOTE, MAX_TARGET, Recorded, note_of};
-pub use auth::{Auth, FETCH_TIMEOUT, KEYS_MAX_AGE, MAX_KEY_DOCUMENT, Principal, REFETCH_AFTER};
+pub use auth::{
+    Auth, CSRF_HEADER, FETCH_TIMEOUT, KEYS_MAX_AGE, MAX_KEY_DOCUMENT, Principal, REFETCH_AFTER,
+};
 pub use cluster::{Control, Stop};
 pub use follow::{FOLLOW_SILENCE, MAX_FRAME};
 pub use history::{MAX_POINTS, MAX_SPAN};
@@ -71,6 +75,7 @@ pub use prometheus::{
 };
 pub use publish::{FIGURES_EVERY, STATE_EVERY, publish};
 pub use server::{Bound, HANDSHAKE_TIMEOUT, IDLE_TIMEOUT, MAX_CONNECTIONS, ServeError, bind};
+pub use session::{COOKIE_CHUNK, KEPT_AFTER, MAX_CHUNKS, SESSION_COOKIE};
 pub use snapshots::{EndedJobs, READ_AT_ONCE};
 pub use stream::{FOLLOW_BACKOFF, KEEP_ALIVE, KINDS};
 pub use tables::{FACTS_FOR, FACTS_WITHIN, Lake};
@@ -907,11 +912,35 @@ async fn misdirected(State(api): State<Api>, req: Request, next: Next) -> Respon
     next.run(req).await
 }
 
+/// The 403 of a request its session authenticates that changes something without the session's
+/// anti-forgery token.
+const FORGED: &str = "a request the console's session authenticates sends the session's anti-forgery token (X-CSRF-Token) when it changes something";
+
+/// Whether a request its session authenticates sent the session's anti-forgery token, as a
+/// request that changes something must: a `GET`, a `HEAD` or an `OPTIONS` needs none.
+pub(crate) fn unforged(method: &Method, headers: &HeaderMap, csrf: &str) -> bool {
+    use subtle::ConstantTimeEq;
+    if matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) {
+        return true;
+    }
+    headers
+        .get(CSRF_HEADER)
+        .is_some_and(|sent| bool::from(sent.as_bytes().ct_eq(csrf.as_bytes())))
+}
+
 /// Lets a caller through to a route that takes `role` or less, with its [`Principal`] in the
 /// request's extensions.
 async fn authorize(State(gate): State<Gate>, mut req: Request, next: Next) -> Response {
     let role = gate.route.role;
-    match gate.auth.verify(req.headers()).await {
+    let verified = gate.auth.verify_caller(req.headers()).await;
+    if let Ok(auth::Verified {
+        session: Some(s), ..
+    }) = &verified
+        && !unforged(req.method(), req.headers(), &s.csrf)
+    {
+        return body(StatusCode::FORBIDDEN, &json!({ "error": FORGED }));
+    }
+    match verified.map(|v| v.principal) {
         Ok(p) if p.role >= role => {
             req.extensions_mut().insert(p);
             next.run(req).await
````

`crates/data-avalanche-api/src/openapi.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/openapi.rs b/crates/data-avalanche-api/src/openapi.rs
index 53c09df..744042d 100644
--- a/crates/data-avalanche-api/src/openapi.rs
+++ b/crates/data-avalanche-api/src/openapi.rs
@@ -10,7 +10,9 @@ use utoipa::openapi::extensions::ExtensionsBuilder;
 use utoipa::openapi::path::{HttpMethod, OperationBuilder, ParameterBuilder, ParameterIn};
 use utoipa::openapi::request_body::RequestBodyBuilder;
 use utoipa::openapi::schema::{ObjectBuilder, Type};
-use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityRequirement, SecurityScheme};
+use utoipa::openapi::security::{
+    ApiKey, ApiKeyValue, HttpAuthScheme, HttpBuilder, SecurityRequirement, SecurityScheme,
+};
 use utoipa::openapi::{
     ComponentsBuilder, ContentBuilder, InfoBuilder, OpenApi, OpenApiBuilder, Paths, Required,
     ResponseBuilder,
@@ -39,6 +41,7 @@ fn operation(r: &Route) -> utoipa::openapi::path::Operation {
         .tag(r.path.split('/').nth(2).unwrap_or("v1"))
         .security(SecurityRequirement::new("basic", Vec::<String>::new()))
         .security(SecurityRequirement::new("bearer", Vec::<String>::new()))
+        .security(SecurityRequirement::new("session", Vec::<String>::new()))
         .extensions(Some(
             ExtensionsBuilder::new().add("x-min-role", role).build(),
         ));
@@ -244,6 +247,13 @@ pub fn openapi() -> OpenApi {
                     .build(),
             ),
         )
+        .security_scheme(
+            "session",
+            SecurityScheme::ApiKey(ApiKey::Cookie(ApiKeyValue::with_description(
+                crate::SESSION_COOKIE,
+                "the web console's session (`globals.console`, with `auth` basic or oauth2), taken when a request has no `Authorization`; a request that changes something sends the session's anti-forgery token in `X-CSRF-Token`",
+            ))),
+        )
         .build();
     OpenApiBuilder::new()
         .info(
````

`crates/data-avalanche-api/src/peers.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/peers.rs b/crates/data-avalanche-api/src/peers.rs
index e5ce7c2..9688373 100644
--- a/crates/data-avalanche-api/src/peers.rs
+++ b/crates/data-avalanche-api/src/peers.rs
@@ -13,6 +13,7 @@ use axum::response::{IntoResponse, Response};
 use data_avalanche_config::TlsIdentity;
 use serde_json::json;
 
+use crate::auth::Presented;
 use crate::body;
 
 /// The node a forwarded request is for: a node that isn't it answers `421`.
@@ -49,24 +50,21 @@ pub enum PeersError {
     Client(String),
 }
 
-/// A request to forward: the caller's method, credentials, content type and body.
+/// A request to forward: the caller's method, credentials, content type and body. The
+/// credentials never print.
 #[derive(Clone)]
 pub(crate) struct Asked {
     pub method: Method,
-    pub authorization: Option<HeaderValue>,
+    pub presented: Presented,
     pub content_type: Option<HeaderValue>,
     pub body: Bytes,
 }
 
-/// The caller's credentials never print.
 impl std::fmt::Debug for Asked {
     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
         f.debug_struct("Asked")
             .field("method", &self.method)
-            .field(
-                "authorization",
-                &self.authorization.as_ref().map(|_| "<redacted>"),
-            )
+            .field("presented", &self.presented)
             .field("content_type", &self.content_type)
             .field("body", &format_args!("{} bytes", self.body.len()))
             .finish()
@@ -78,7 +76,7 @@ impl Asked {
     pub(crate) fn from(method: Method, headers: &HeaderMap, body: Bytes) -> Self {
         Self {
             method,
-            authorization: headers.get(header::AUTHORIZATION).cloned(),
+            presented: Presented::of(headers),
             content_type: headers.get(header::CONTENT_TYPE).cloned(),
             body,
         }
@@ -126,12 +124,10 @@ impl Peers {
             .http
             .request(asked.method, format!("{api_url}{path_and_query}"))
             .timeout(limit)
+            .headers(asked.presented.headers())
             .header(TARGET, node)
             .header(FORWARDED_BY, &self.me)
             .body(asked.body);
-        if let Some(a) = asked.authorization {
-            req = req.header(header::AUTHORIZATION, a);
-        }
         if let Some(c) = asked.content_type {
             req = req.header(header::CONTENT_TYPE, c);
         }
````

`crates/data-avalanche-api/src/session.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/session.rs b/crates/data-avalanche-api/src/session.rs
index b971e32..ef06425 100644
--- a/crates/data-avalanche-api/src/session.rs
+++ b/crates/data-avalanche-api/src/session.rs
@@ -1,3 +1,349 @@
+//! The console's session (Admin_Console §9): with `auth` `basic` or `oauth2`, a person signed in
+//! holds an encrypted cookie that every node opens with `globals.console.session_key`, so a load
+//! balancer may send any request anywhere. It carries what verifies the person again on every
+//! request (the basic user and a fingerprint of the password; the provider's access token, and
+//! its refresh token when one was issued), when they signed in, and the anti-forgery token their
+//! mutating requests send back. Nothing of it is kept anywhere else: a key change ends every
+//! session, since no node opens the old cookies any more.
+
+use std::fmt::Write as _;
+
+use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
+use axum::http::HeaderMap;
+use axum::http::header::COOKIE;
+use base64::Engine;
+use base64::engine::general_purpose::URL_SAFE_NO_PAD;
+use serde::{Deserialize, Serialize};
+
+/// The session cookie; one too long for a cookie goes on in `da_session.1`, `da_session.2`, ….
+pub const SESSION_COOKIE: &str = "da_session";
+/// A cookie's value is at most this long: browsers keep a cookie of 4096 bytes, its name and
+/// attributes included.
+pub const COOKIE_CHUNK: usize = 3800;
+/// A session takes at most this many cookies: a provider whose tokens need more can't start one.
+pub const MAX_CHUNKS: usize = 4;
+/// A session's cookies outlive it by this long, so its page can still say when it ended.
+pub const KEPT_AFTER: std::time::Duration = std::time::Duration::from_secs(24 * 3600);
+
+/// The sealed format's version, its first byte.
+const VERSION: u8 = 1;
+/// The key's id: a cookie sealed with another key is told from one altered.
+const ID_LEN: usize = 8;
+
+/// Seals and opens what the console keeps in cookies, with keys derived from the session key:
+/// AES-256-GCM, a random nonce for each seal, and the cookie's purpose as associated data, so a
+/// value sealed for one purpose doesn't open as another.
+pub(crate) struct Sealer {
+    key: LessSafeKey,
+    id: [u8; ID_LEN],
+    fingerprints: [u8; 32],
+}
+
+impl std::fmt::Debug for Sealer {
+    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
+        f.debug_struct("Sealer").finish_non_exhaustive()
+    }
+}
+
+/// Why a sealed value doesn't open.
+#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
+pub(crate) enum Unsealed {
+    /// Sealed with another session key: the key changed since.
+    #[error("the session key changed")]
+    KeyChanged,
+    /// Not sealed by this cluster, altered, or sealed for another purpose.
+    #[error("the session cookie doesn't decrypt")]
+    Garbled,
+}
+
+impl Sealer {
+    /// The sealer of `globals.console.session_key` (at least 32 bytes, V-128).
+    pub(crate) fn new(session_key: &str) -> Self {
+        let derived = |context: &str| blake3::derive_key(context, session_key.as_bytes());
+        let key = UnboundKey::new(
+            &AES_256_GCM,
+            &derived("data-avalanche 2026-11-24 console cookie encryption"),
+        )
+        .expect("a 32-byte AES-256 key");
+        let mut id = [0; ID_LEN];
+        id.copy_from_slice(&derived("data-avalanche 2026-11-24 console cookie key id")[..ID_LEN]);
+        Self {
+            key: LessSafeKey::new(key),
+            id,
+            fingerprints: derived("data-avalanche 2026-11-24 console secret fingerprint"),
+        }
+    }
+
+    /// `plain`, sealed for `purpose`, as URL-safe base64.
+    #[cfg_attr(
+        not(test),
+        expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
+    )]
+    pub(crate) fn seal(&self, purpose: &str, plain: &[u8]) -> String {
+        let mut nonce = [0; NONCE_LEN];
+        aws_lc_rs::rand::fill(&mut nonce).expect("the system's random source");
+        let mut sealed = plain.to_vec();
+        self.key
+            .seal_in_place_append_tag(
+                Nonce::assume_unique_for_key(nonce),
+                Aad::from(purpose.as_bytes()),
+                &mut sealed,
+            )
+            .expect("AES-GCM seals any length a cookie takes");
+        let mut out = Vec::with_capacity(1 + ID_LEN + NONCE_LEN + sealed.len());
+        out.push(VERSION);
+        out.extend_from_slice(&self.id);
+        out.extend_from_slice(&nonce);
+        out.extend_from_slice(&sealed);
+        URL_SAFE_NO_PAD.encode(out)
+    }
+
+    /// What [`Self::seal`] sealed for `purpose`.
+    ///
+    /// # Errors
+    /// [`Unsealed`]: sealed with another key, or not by this one for `purpose`.
+    pub(crate) fn open(&self, purpose: &str, sealed: &str) -> Result<Vec<u8>, Unsealed> {
+        let raw = URL_SAFE_NO_PAD
+            .decode(sealed)
+            .map_err(|_| Unsealed::Garbled)?;
+        if raw.len() < 1 + ID_LEN + NONCE_LEN || raw[0] != VERSION {
+            return Err(Unsealed::Garbled);
+        }
+        if raw[1..=ID_LEN] != self.id {
+            return Err(Unsealed::KeyChanged);
+        }
+        let mut nonce = [0; NONCE_LEN];
+        nonce.copy_from_slice(&raw[1 + ID_LEN..1 + ID_LEN + NONCE_LEN]);
+        let mut body = raw[1 + ID_LEN + NONCE_LEN..].to_vec();
+        let plain = self
+            .key
+            .open_in_place(
+                Nonce::assume_unique_for_key(nonce),
+                Aad::from(purpose.as_bytes()),
+                &mut body,
+            )
+            .map_err(|_| Unsealed::Garbled)?;
+        Ok(plain.to_vec())
+    }
+
+    /// A keyed fingerprint of `secret`: a session holds the basic password's, so a password
+    /// changed ends it, and holds nothing the password could be read back from.
+    pub(crate) fn fingerprint(&self, secret: &str) -> String {
+        URL_SAFE_NO_PAD.encode(blake3::keyed_hash(&self.fingerprints, secret.as_bytes()).as_bytes())
+    }
+}
+
+/// A random token, URL-safe: 32 bytes of the system's random source.
+#[cfg_attr(
+    not(test),
+    expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
+)]
+pub(crate) fn random_token() -> String {
+    let mut b = [0; 32];
+    aws_lc_rs::rand::fill(&mut b).expect("the system's random source");
+    URL_SAFE_NO_PAD.encode(b)
+}
+
+/// A signed-in person's session.
+#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
+pub(crate) struct Session {
+    /// When they signed in, Unix seconds: the session ends `session_ttl` after, whatever they
+    /// do (S-M14.1).
+    pub signed_in: i64,
+    /// The anti-forgery token: a mutating request the session authenticates sends it back.
+    pub csrf: String,
+    /// What verifies them again.
+    pub by: By,
+}
+
+/// The anti-forgery token never prints.
+impl std::fmt::Debug for Session {
+    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
+        f.debug_struct("Session")
+            .field("signed_in", &self.signed_in)
+            .field("by", &self.by)
+            .finish_non_exhaustive()
+    }
+}
+
+/// What a session verifies its person by, again on each request.
+#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(tag = "mode", rename_all = "lowercase")]
+pub(crate) enum By {
+    /// The configured user, and the [`Sealer::fingerprint`] of the password they gave.
+    Basic { user: String, fingerprint: String },
+    /// The provider's access token, verified as a bearer token is; its refresh token, when it
+    /// issued one.
+    Oauth2 {
+        access: String,
+        refresh: Option<String>,
+    },
+}
+
+/// The tokens never print.
+impl std::fmt::Debug for By {
+    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
+        match self {
+            Self::Basic { user, .. } => f
+                .debug_struct("Basic")
+                .field("user", user)
+                .finish_non_exhaustive(),
+            Self::Oauth2 { refresh, .. } => f
+                .debug_struct("Oauth2")
+                .field("refresh", &refresh.as_ref().map(|_| "<redacted>"))
+                .finish_non_exhaustive(),
+        }
+    }
+}
+
+/// A session too long for [`MAX_CHUNKS`] cookies.
+#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
+#[cfg_attr(
+    not(test),
+    expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
+)]
+#[error(
+    "the session needs {0} bytes of cookies, more than the {MAX_CHUNKS} cookies of {COOKIE_CHUNK} bytes a browser is sent: the provider's tokens are too long"
+)]
+pub(crate) struct TooLong(pub usize);
+
+/// The purpose a session is sealed for.
+const SESSION: &str = "session";
+
+impl Session {
+    /// A session signed in at `signed_in`, with a new anti-forgery token.
+    #[cfg_attr(
+        not(test),
+        expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
+    )]
+    pub(crate) fn new(by: By, signed_in: i64) -> Self {
+        Self {
+            signed_in,
+            csrf: random_token(),
+            by,
+        }
+    }
+
+    /// The `Set-Cookie` values that hold this session, kept `max_age` seconds: its cookies, then
+    /// the removal of each further one an earlier, longer session may have left.
+    ///
+    /// # Errors
+    /// [`TooLong`]: it needs more than [`MAX_CHUNKS`] cookies.
+    #[cfg_attr(
+        not(test),
+        expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
+    )]
+    pub(crate) fn cookies(&self, sealer: &Sealer, max_age: u64) -> Result<Vec<String>, TooLong> {
+        let plain = serde_json::to_vec(self).expect("a session serializes");
+        let sealed = sealer.seal(SESSION, &plain);
+        let chunks: Vec<&str> = sealed
+            .as_bytes()
+            .chunks(COOKIE_CHUNK)
+            .map(|c| std::str::from_utf8(c).expect("base64 is ASCII"))
+            .collect();
+        if chunks.len() > MAX_CHUNKS {
+            return Err(TooLong(sealed.len()));
+        }
+        let mut out: Vec<String> = chunks
+            .iter()
+            .enumerate()
+            .map(|(i, c)| set_cookie(&chunk_name(i), c, "/", max_age, "Strict"))
+            .collect();
+        out.extend((chunks.len()..MAX_CHUNKS).map(|i| removed(&chunk_name(i), "/")));
+        Ok(out)
+    }
+
+    /// The session the request's cookies hold: `None` without its cookie.
+    ///
+    /// # Errors
+    /// [`Unsealed`]: its cookies don't open, or what they hold isn't a session.
+    pub(crate) fn of(headers: &HeaderMap, sealer: &Sealer) -> Option<Result<Self, Unsealed>> {
+        let sealed = sealed_value(headers)?;
+        Some(
+            sealer
+                .open(SESSION, &sealed)
+                .and_then(|plain| serde_json::from_slice(&plain).map_err(|_| Unsealed::Garbled)),
+        )
+    }
+}
+
+/// The session's cookies, joined in order: `None` without its first.
+pub(crate) fn sealed_value(headers: &HeaderMap) -> Option<String> {
+    let mut sealed = String::new();
+    for i in 0..MAX_CHUNKS {
+        match cookie(headers, &chunk_name(i)) {
+            Some(v) => sealed.push_str(v),
+            None if i == 0 => return None,
+            None => break,
+        }
+    }
+    Some(sealed)
+}
+
+/// The request's session cookies alone, as one `Cookie` header's value: what a node passes on to
+/// a peer with the caller's other credentials (Admin_Console §10).
+pub(crate) fn session_cookies(headers: &HeaderMap) -> Option<String> {
+    let mut out = String::new();
+    for i in 0..MAX_CHUNKS {
+        let name = chunk_name(i);
+        let Some(v) = cookie(headers, &name) else {
+            break;
+        };
+        if !out.is_empty() {
+            out.push_str("; ");
+        }
+        let _ = write!(out, "{name}={v}");
+    }
+    (!out.is_empty()).then_some(out)
+}
+
+fn chunk_name(i: usize) -> String {
+    if i == 0 {
+        SESSION_COOKIE.to_owned()
+    } else {
+        format!("{SESSION_COOKIE}.{i}")
+    }
+}
+
+/// The value of cookie `name` in the request's `Cookie` headers.
+pub(crate) fn cookie<'h>(headers: &'h HeaderMap, name: &str) -> Option<&'h str> {
+    headers
+        .get_all(COOKIE)
+        .iter()
+        .filter_map(|v| v.to_str().ok())
+        .flat_map(|v| v.split(';'))
+        .filter_map(|pair| pair.trim().split_once('='))
+        .find(|(n, _)| *n == name)
+        .map(|(_, v)| v)
+}
+
+/// A `Set-Cookie` value: `HttpOnly` and `Secure`, for `path`, kept `max_age` seconds, `same_site`
+/// `Strict` or `Lax`.
+#[cfg_attr(
+    not(test),
+    expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
+)]
+pub(crate) fn set_cookie(
+    name: &str,
+    value: &str,
+    path: &str,
+    max_age: u64,
+    same_site: &str,
+) -> String {
+    format!(
+        "{name}={value}; Path={path}; Max-Age={max_age}; HttpOnly; Secure; SameSite={same_site}"
+    )
+}
+
+/// The `Set-Cookie` value that removes cookie `name` at `path`.
+#[cfg_attr(
+    not(test),
+    expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
+)]
+pub(crate) fn removed(name: &str, path: &str) -> String {
+    format!("{name}=; Path={path}; Max-Age=0; HttpOnly; Secure; SameSite=Strict")
+}
+
 #[cfg(test)]
 mod tests {
     use axum::http::HeaderValue;
````

`crates/data-avalanche-api/src/stream.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/stream.rs b/crates/data-avalanche-api/src/stream.rs
index 27264ec..6ed5413 100644
--- a/crates/data-avalanche-api/src/stream.rs
+++ b/crates/data-avalanche-api/src/stream.rs
@@ -27,7 +27,7 @@ use serde_json::{Value, json};
 use tokio::sync::{Notify, broadcast, mpsc, watch};
 use tokio::task::{AbortHandle, JoinSet};
 
-use crate::auth::Denied;
+use crate::auth::{Denied, Presented};
 use crate::follow::FOLLOW_SILENCE;
 use crate::nodes::phase;
 use crate::peers::FORWARDED_BY;
@@ -207,7 +207,7 @@ pub(crate) async fn open(
             } else {
                 format!("/v1/stream?{raw}")
             },
-            authorization: headers.get(header::AUTHORIZATION).cloned(),
+            presented: Presented::of(&headers),
         })
     });
     let (tx, rx) = mpsc::channel(BUFFER);
@@ -233,13 +233,13 @@ async fn deliver(tx: &mpsc::Sender<Event>, opened: &mut Opened, e: Event) -> boo
 }
 
 /// What a stream follows its peers with: the node's members and client, the API's own check of
-/// a caller, the stream's own path and query, and the caller's credentials (its `Authorization`
-/// alone, never logged).
+/// a caller, the stream's own path and query, and the credentials the caller presented (never
+/// logged).
 struct Follow {
     node: Node,
     auth: Arc<Auth>,
     path: String,
-    authorization: Option<HeaderValue>,
+    presented: Presented,
 }
 
 impl Follow {
@@ -248,11 +248,7 @@ impl Follow {
     /// check now (its settings refused, the issuer's keys unreachable: a 503) refuses no caller:
     /// the peer is followed, and answers for itself.
     async fn still_verified(&self) -> bool {
-        let mut headers = HeaderMap::new();
-        if let Some(a) = &self.authorization {
-            headers.insert(header::AUTHORIZATION, a.clone());
-        }
-        match self.auth.verify(&headers).await {
+        match self.auth.verify(&self.presented.headers()).await {
             Ok(p) => p.role >= Role::Viewer,
             Err(Denied::Unavailable(_)) => true,
             Err(Denied::Unauthenticated { .. } | Denied::Forbidden { .. }) => false,
@@ -337,13 +333,7 @@ async fn follow_peer(
         let error = match f
             .node
             .peers
-            .follow(
-                &peer,
-                &url,
-                &f.path,
-                f.authorization.clone(),
-                FOLLOW_SILENCE,
-            )
+            .follow(&peer, &url, &f.path, &f.presented, FOLLOW_SILENCE)
             .await
         {
             Err(e) => e,
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index ad68551..165a412 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -147,7 +147,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     let ready = Arc::new(AtomicBool::new(false));
     let node = Arc::new(OnceLock::new());
     let api = &first.config.globals.api;
-    let auth = Arc::new(Auth::new(api, &first.config.globals.console.username_claim));
+    let auth = Arc::new(Auth::new(api, &first.config.globals.console));
     let bound = bind(&api.address, api.tls.as_ref())
         .await
         .map_err(|e| match e {
@@ -171,7 +171,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     let config = joined.config;
     logger.apply(&config.globals.log);
     logger.trace(&config.globals.tracing);
-    auth.set(&config.globals.api, &config.globals.console.username_claim);
+    auth.set(&config.globals.api, &config.globals.console);
     tracing::info!(
         node = %holder.node_id,
         incarnation = holder.incarnation,
@@ -306,7 +306,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
             globals: Arc::new(move |g| {
                 log.apply(&g.log);
                 log.trace(&g.tracing);
-                reloaded_auth.set(&g.api, &g.console.username_claim);
+                reloaded_auth.set(&g.api, &g.console);
                 retention.send_replace(g.console.events.retention);
                 history_retention.send_replace(g.console.history.retention);
             }),
````

Then regenerate `Cargo.lock`, `crates/data-avalanche-api/openapi.json` and
`docs/public/openapi.json`: `cargo build` updates `Cargo.lock` (the API crate's `aws-lc-rs`);
`DATA_AVALANCHE_BLESS=1 cargo test -p data-avalanche-api --lib openapi::` writes the document
(check its diff is the `session` scheme and each operation's security naming it), then
`cargo xtask docs` writes the site's copy.

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-api --lib`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock Cargo.toml crates/data-avalanche-api/Cargo.toml crates/data-avalanche-api/openapi.json crates/data-avalanche-api/src/actions.rs crates/data-avalanche-api/src/auth.rs crates/data-avalanche-api/src/follow.rs crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/nodes.rs crates/data-avalanche-api/src/openapi.rs crates/data-avalanche-api/src/peers.rs crates/data-avalanche-api/src/session.rs crates/data-avalanche-api/src/stream.rs crates/data-avalanche-api/src/test_support.rs crates/data-avalanche/src/run.rs docs/public/openapi.json
git commit -m "feat(api): console sessions every node opens; the anti-forgery check; sessions passed to peers"
```

---

### Task 2: A stream ends with its caller's credentials (`data-avalanche-api`)

**Files:**
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Modify: `crates/data-avalanche-api/src/stream.rs`

**Interfaces:**
- Consumes: `Auth::verify_caller`, `Principal::expires`, `Presented` (Task 1); the stream's
  `credentials` reset (M13f's whole-branch fixes).
- Produces: `CREDENTIALS_EVERY` (60 s, crate root).

- [ ] **Step 1: Write the failing tests**

The test: `a_stream_ends_when_its_session_does` (stream): a stream opened with a session whose
`session_ttl` ends while it is open sends `reset` (`credentials`) a second after the end, and
ends. It replaces the module's `basic` helper with Task 1's `basic_api`.

`crates/data-avalanche-api/src/stream.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/stream.rs b/crates/data-avalanche-api/src/stream.rs
index 6ed5413..f1a4195 100644
--- a/crates/data-avalanche-api/src/stream.rs
+++ b/crates/data-avalanche-api/src/stream.rs
@@ -470,7 +470,9 @@ mod tests {
     use data_avalanche_cluster::FEED_CAPACITY;
 
     use super::*;
-    use crate::test_support::{Stream, served_with, stream};
+    use crate::test_support::{
+        Stream, basic_api, basic_session, served_with, session_cookie, sessions, stream,
+    };
 
     fn live(kind: &'static str, pipeline: Option<&str>, n: u64) -> Live {
         Live {
@@ -795,18 +797,6 @@ mod tests {
         assert_eq!(log.lock().unwrap().len(), 2);
     }
 
-    /// `globals.api` with `auth: basic`, its one user `ops` with `password`.
-    fn basic(password: &str) -> data_avalanche_config::Api {
-        use data_avalanche_config::{ApiAuth, Credentials, FingerprintKey, Secret};
-        let mut api = crate::test_support::config().globals.api;
-        api.auth = ApiAuth::Basic;
-        api.credentials = Some(Credentials::Inline {
-            user: "ops".into(),
-            password: Secret::new(password, &FingerprintKey::from_bytes([1; 32])),
-        });
-        api
-    }
-
     /// `ops:s3cret`, as a Basic `Authorization`.
     const OPS: &str = "Basic b3BzOnMzY3JldA==";
 
@@ -819,7 +809,7 @@ mod tests {
         )
         .await;
         let auth = std::sync::Arc::new(crate::Auth::new(
-            &basic("s3cret"),
+            &basic_api("s3cret"),
             &crate::test_support::console(),
         ));
         let (addr, _lone, _dir) = served_with(&b_url, std::sync::Arc::clone(&auth), |_| {}).await;
@@ -828,7 +818,7 @@ mod tests {
         s.next().await;
         assert_eq!(s.next().await.1["state"], "unreachable");
         // The password changes before node-b is followed again (FOLLOW_BACKOFF[0]).
-        auth.set(&basic("rotated"), &crate::test_support::console());
+        auth.set(&basic_api("rotated"), &crate::test_support::console());
         assert_eq!(
             s.next().await,
             (
@@ -851,7 +841,7 @@ mod tests {
         )
         .await;
         let auth = std::sync::Arc::new(crate::Auth::new(
-            &basic("s3cret"),
+            &basic_api("s3cret"),
             &crate::test_support::console(),
         ));
         let (addr, lone, _dir) = served_with(&b_url, std::sync::Arc::clone(&auth), |_| {}).await;
@@ -865,7 +855,7 @@ mod tests {
         });
         assert_eq!(s.next().await, ("peer".into(), unreachable.clone()));
         // `auth: basic` without its credentials: every caller is answered 503, none refused.
-        let mut refused = basic("s3cret");
+        let mut refused = basic_api("s3cret");
         refused.credentials = None;
         auth.set(&refused, &crate::test_support::console());
         assert_eq!(
@@ -887,7 +877,7 @@ mod tests {
         )
         .await;
         let auth = std::sync::Arc::new(crate::Auth::new(
-            &basic("s3cret"),
+            &basic_api("s3cret"),
             &crate::test_support::console(),
         ));
         let (addr, lone, _dir) = served_with(&b_url, auth, |_| {}).await;
@@ -906,6 +896,41 @@ mod tests {
         assert_eq!(s.next().await.1["n"], 1, "the stream goes on");
     }
 
+    /// M13f's carry-in: a stream ends with its caller's session, and a token's stream with its
+    /// token, at their end; the page opens it again with fresh credentials.
+    #[tokio::test]
+    async fn a_stream_ends_when_its_session_does() {
+        let hour = Duration::from_secs(3600);
+        let auth = std::sync::Arc::new(crate::Auth::new(&basic_api("s3cret"), &sessions(hour)));
+        let (addr, lone, _dir) = served_with("", auth, |_| {}).await;
+        // It ends within three seconds.
+        let session = basic_session("s3cret", hour - Duration::from_secs(2));
+        let cookie = session_cookie(&session);
+        let opened = std::time::Instant::now();
+        let mut s = stream(addr, "/v1/stream", &[("cookie", cookie.as_str())]).await;
+        assert_eq!(s.code, 200);
+        hello(&mut s).await;
+        lone.node.feed.publish(live("node", None, 1));
+        assert_eq!(
+            s.next().await.1["n"],
+            1,
+            "it streams while the session lasts"
+        );
+        assert_eq!(
+            s.next().await,
+            (
+                "reset".into(),
+                json!({ "node": "node-a", "reason": "credentials" })
+            )
+        );
+        assert!(s.ended().await, "the stream ends: the page opens it again");
+        let took = opened.elapsed();
+        assert!(
+            took >= Duration::from_secs(1) && took < Duration::from_secs(8),
+            "{took:?}"
+        );
+    }
+
     #[tokio::test]
     async fn a_stream_another_node_asked_for_follows_none() {
         let b = node_b().await;
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api --lib -- stream::`
Expected: the test compiles and fails: the stream sends nothing when the session ends (`a message within 5 s: Elapsed(())`); the stream's other 17 tests pass.

- [ ] **Step 3: Implement**

The stream's loop re-verifies the caller's kept headers with `verify_caller` at
`CREDENTIALS_EVERY`, or a second after `Principal::expires` when that comes first, and on a
refusal sends `reset` (`credentials`) and ends; an unavailable verifier refuses no one.

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 3dfe4aa..15167ff 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -77,7 +77,7 @@ pub use publish::{FIGURES_EVERY, STATE_EVERY, publish};
 pub use server::{Bound, HANDSHAKE_TIMEOUT, IDLE_TIMEOUT, MAX_CONNECTIONS, ServeError, bind};
 pub use session::{COOKIE_CHUNK, KEPT_AFTER, MAX_CHUNKS, SESSION_COOKIE};
 pub use snapshots::{EndedJobs, READ_AT_ONCE};
-pub use stream::{FOLLOW_BACKOFF, KEEP_ALIVE, KINDS};
+pub use stream::{CREDENTIALS_EVERY, FOLLOW_BACKOFF, KEEP_ALIVE, KINDS};
 pub use tables::{FACTS_FOR, FACTS_WITHIN, Lake};
 
 use crate::auth::Denied;
````

`crates/data-avalanche-api/src/stream.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/stream.rs b/crates/data-avalanche-api/src/stream.rs
index f1a4195..7a35d2b 100644
--- a/crates/data-avalanche-api/src/stream.rs
+++ b/crates/data-avalanche-api/src/stream.rs
@@ -16,18 +16,20 @@ use std::time::Duration;
 
 use std::collections::BTreeMap;
 
+use axum::Extension;
 use axum::extract::{RawQuery, State};
 use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
 use axum::response::sse::{Event, KeepAlive, Sse};
 use axum::response::{IntoResponse, Response};
-use chrono::Utc;
+use chrono::{DateTime, Utc};
 use data_avalanche_cluster::{Live, MAX_STREAMS, Members, Opened};
 use data_avalanche_config::{ResolvedConfig, Role};
 use serde_json::{Value, json};
 use tokio::sync::{Notify, broadcast, mpsc, watch};
 use tokio::task::{AbortHandle, JoinSet};
+use tokio::time::Instant;
 
-use crate::auth::{Denied, Presented};
+use crate::auth::{Denied, Presented, Principal};
 use crate::follow::FOLLOW_SILENCE;
 use crate::nodes::phase;
 use crate::peers::FORWARDED_BY;
@@ -162,9 +164,16 @@ fn event(kind: &str, data: &Value) -> Event {
     Event::default().event(kind).data(data.to_string())
 }
 
+/// An open stream checks its caller again this often, and at its credentials' expiry when that
+/// comes first: once they no longer verify (a session or a token ended, a password changed, a
+/// role taken away), it is sent `reset` (`credentials`) and ends, and the page opens it again
+/// with fresh credentials.
+pub const CREDENTIALS_EVERY: Duration = Duration::from_secs(60);
+
 /// `GET /v1/stream`.
 pub(crate) async fn open(
     State(api): State<Api>,
+    Extension(caller): Extension<Principal>,
     headers: HeaderMap,
     RawQuery(raw): RawQuery,
 ) -> Response {
@@ -208,6 +217,7 @@ pub(crate) async fn open(
                 format!("/v1/stream?{raw}")
             },
             presented: Presented::of(&headers),
+            expires: caller.expires,
         })
     });
     let (tx, rx) = mpsc::channel(BUFFER);
@@ -233,13 +243,14 @@ async fn deliver(tx: &mpsc::Sender<Event>, opened: &mut Opened, e: Event) -> boo
 }
 
 /// What a stream follows its peers with: the node's members and client, the API's own check of
-/// a caller, the stream's own path and query, and the credentials the caller presented (never
-/// logged).
+/// a caller, the stream's own path and query, the credentials the caller presented (never
+/// logged), and when they expire.
 struct Follow {
     node: Node,
     auth: Arc<Auth>,
     path: String,
     presented: Presented,
+    expires: Option<DateTime<Utc>>,
 }
 
 impl Follow {
@@ -254,6 +265,25 @@ impl Follow {
             Err(Denied::Unauthenticated { .. } | Denied::Forbidden { .. }) => false,
         }
     }
+
+    /// When the caller is checked again: [`CREDENTIALS_EVERY`] from now, or a second after its
+    /// credentials expire when that comes first.
+    fn next_check(&self) -> Instant {
+        let every = Instant::now() + CREDENTIALS_EVERY;
+        self.expires
+            .and_then(|at| (at - Utc::now()).to_std().ok())
+            .map_or(every, |left| {
+                every.min(Instant::now() + left + Duration::from_secs(1))
+            })
+    }
+}
+
+/// Resolves at `when`; never without one.
+async fn at(when: Option<Instant>) {
+    match when {
+        Some(when) => tokio::time::sleep_until(when).await,
+        None => std::future::pending().await,
+    }
 }
 
 /// Each other member with an API address, by name.
@@ -385,11 +415,14 @@ enum Next {
     Live(Result<Arc<Live>, broadcast::error::RecvError>),
     /// The caller's credentials no longer verify.
     Refused,
+    /// Time to check the caller again.
+    Check,
 }
 
 /// Sends `hello`, then this node's messages that pass `filter` and, with `follow`, each peer's,
 /// until the client goes or the node stops; or, once the caller's credentials no longer verify
-/// when a peer is followed again, `reset` (`credentials`), and ends.
+/// (checked every [`CREDENTIALS_EVERY`], at their expiry, and before a peer is followed again),
+/// `reset` (`credentials`), and ends.
 async fn pump(
     me: String,
     filter: Filter,
@@ -410,6 +443,7 @@ async fn pump(
     let mut following = Following::default();
     let refused = Arc::clone(&following.refused);
     let mut shown = view.as_ref().map(listed);
+    let mut check = follow.as_ref().map(|f| f.next_check());
     if let (Some(f), Some(view)) = (&follow, &view) {
         following.reconcile(f, view, &tx);
     }
@@ -418,10 +452,23 @@ async fn pump(
             () = tx.closed() => return,
             _ = opened.closed.wait_for(|c| *c) => return,
             () = refused.notified() => Next::Refused,
+            () = at(check) => Next::Check,
             Some(()) = changed(&mut members) => Next::Members,
             r = opened.messages.recv() => Next::Live(r),
         };
         let next = match next {
+            Next::Check => match &follow {
+                Some(f) if !f.still_verified().await => Next::Refused,
+                Some(f) => {
+                    check = Some(f.next_check());
+                    continue;
+                }
+                None => continue,
+            },
+            other => other,
+        };
+        let next = match next {
+            Next::Check => continue,
             Next::Live(r) => r,
             Next::Refused => {
                 // Its peers' messages end first, so `reset` is the stream's last.
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-api --lib -- stream::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/stream.rs
git commit -m "feat(api): a stream ends with its caller's credentials"
```

---

### Task 3: The console's routes under `/ui/`: the `basic` sign-in, the 401 and 403 pages (`data-avalanche-api`)

**Files:**
- Create: `crates/data-avalanche-api/assets/console.css`
- Modify: `crates/data-avalanche-api/src/auth.rs`
- Create: `crates/data-avalanche-api/src/console.rs`
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Create: `crates/data-avalanche-api/src/pages.rs`
- Modify: `crates/data-avalanche-api/src/server.rs`
- Modify: `crates/data-avalanche-api/src/session.rs`

**Interfaces:**
- Consumes: `Auth::verify_caller`, `Verified`, `Sessions`, `Session`, `Sealer`, the cookie
  helpers (Task 1).
- Produces (crate root): `SIGN_IN_ATTEMPTS` (5), `SIGN_IN_WAIT` (60 s).
- Produces (crate-private):
  - `console::routes(&Api) -> Router<Api>`, merged into the router; `safe_next(Option<&str>) ->
    String`; `started(&Session, &Sessions, next) -> Response`; `Attempts::{check(ip, now) ->
    Result<(), Duration>, failed(ip, now) -> u32, succeeded(ip)}`;
  - `Auth::sign_in() -> Option<SignIn>` (`None` while the console is off),
    `SignIn { auth, sessions, credentials, .. }`, `Auth::attempts`; `basic_matches(&Credentials,
    user, password) -> bool`;
  - `server::Client(SocketAddr)`, the connection's address (`ConnectInfo<Client>`);
  - `Session::removed() -> Vec<String>` (every session cookie, `Max-Age=0`);
  - `pages::{Frame, escape, basic_sign_in, ended, no_token, forbidden, unavailable, waiting,
    cross_site, signed_in, encode}`, each returning the page's HTML.

- [ ] **Step 1: Write the failing tests**

The tests (console): `the_console_is_served_only_while_it_is_enabled`,
`a_basic_sign_in_comes_back_to_the_page_asked_for`,
`wrong_passwords_leave_fewer_attempts_then_a_wait`,
`failures_are_counted_by_address_and_a_success_starts_again`,
`an_ended_session_says_why_and_leads_back`, `with_jwt_a_page_needs_a_valid_token`,
`with_none_every_page_warns_there_is_no_authentication`, `a_form_another_site_posts_is_refused`,
`next_is_always_a_console_page`, `pages_escape_what_they_show`,
`the_stylesheet_is_served_and_nothing_else`. The new module holds only its tests for now.

`crates/data-avalanche-api/src/console.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/console.rs b/crates/data-avalanche-api/src/console.rs
new file mode 100644
index 0000000..30368c3
--- /dev/null
+++ b/crates/data-avalanche-api/src/console.rs
@@ -0,0 +1,366 @@
+#[cfg(test)]
+mod tests {
+    use std::net::SocketAddr;
+    use std::sync::Arc;
+
+    use data_avalanche_config::Console;
+    use serde_json::Value;
+
+    use super::*;
+    use crate::Auth;
+    use crate::test_support::{
+        Issuer, api, basic_api, basic_session, config, console, request, serve, session_cookie,
+        sessions,
+    };
+
+    const HOUR: Duration = Duration::from_secs(3600);
+
+    /// The console enabled without sessions, as `none` and `jwt` run it.
+    fn enabled() -> Console {
+        Console {
+            enabled: true,
+            ..console()
+        }
+    }
+
+    /// A node serving `globals.api` and `globals.console` as given.
+    async fn node(api_config: &data_avalanche_config::Api, c: &Console) -> SocketAddr {
+        serve(api(Arc::new(Auth::new(api_config, c)))).await
+    }
+
+    /// The value of header `name` in `head`, in lower case.
+    fn header_of<'h>(head: &'h str, name: &str) -> Vec<&'h str> {
+        head.lines()
+            .filter_map(|l| l.split_once(": "))
+            .filter(|(n, _)| n.eq_ignore_ascii_case(name))
+            .map(|(_, v)| v)
+            .collect()
+    }
+
+    /// The `Cookie` a browser sends back after `head`'s `Set-Cookie`s.
+    fn kept(head: &str) -> String {
+        header_of(head, "set-cookie")
+            .into_iter()
+            .filter(|c| !c.contains("Max-Age=0"))
+            .map(|c| c.split(';').next().unwrap())
+            .collect::<Vec<_>>()
+            .join("; ")
+    }
+
+    /// Admin_Console §3: the console's routes answer only while it is on; a reload turns it on.
+    #[tokio::test]
+    async fn the_console_is_served_only_while_it_is_enabled() {
+        let auth = Arc::new(Auth::new(&config().globals.api, &console()));
+        let addr = serve(api(Arc::clone(&auth))).await;
+        for path in [
+            "/ui/",
+            "/ui/sign-in",
+            "/ui/session",
+            "/ui/assets/console.css",
+        ] {
+            let (code, _, body) = request(addr, "GET", path, &[], "").await;
+            assert_eq!(code, 404, "{path}");
+            assert!(body.contains("globals.console.enabled"), "{body}");
+        }
+        auth.set(&config().globals.api, &enabled());
+        let (code, head, body) = request(addr, "GET", "/ui/", &[], "").await;
+        assert_eq!(code, 200, "{body}");
+        assert_eq!(
+            header_of(&head, "content-security-policy"),
+            [
+                "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"
+            ]
+        );
+        assert_eq!(header_of(&head, "x-frame-options"), ["DENY"]);
+        assert_eq!(header_of(&head, "x-content-type-options"), ["nosniff"]);
+        assert_eq!(header_of(&head, "referrer-policy"), ["no-referrer"]);
+        assert_eq!(header_of(&head, "cache-control"), ["no-store"]);
+        let (code, head, _) = request(addr, "GET", "/ui", &[], "").await;
+        assert_eq!((code, header_of(&head, "location")), (303, vec!["/ui/"]));
+    }
+
+    /// Admin_Console §9, `basic`: a page asked for without a session goes to the sign-in form,
+    /// and the right password comes back to it signed in, with a session every page reads.
+    #[tokio::test]
+    async fn a_basic_sign_in_comes_back_to_the_page_asked_for() {
+        let addr = node(&basic_api("s3cret"), &sessions(8 * HOUR)).await;
+        let (code, head, _) =
+            request(addr, "GET", "/ui/pipelines/orders?tab=tables", &[], "").await;
+        assert_eq!(code, 303);
+        let to = "/ui/sign-in?next=%2Fui%2Fpipelines%2Forders%3Ftab%3Dtables";
+        assert_eq!(header_of(&head, "location"), [to]);
+        let (code, _, page) = request(addr, "GET", to, &[], "").await;
+        assert_eq!(code, 200);
+        assert!(page.contains("Sign in to the cluster"), "{page}");
+        assert!(
+            page.contains(r#"name="next" value="/ui/pipelines/orders?tab=tables""#),
+            "{page}"
+        );
+        let form = "user=ops&password=s3cret&next=%2Fui%2Fpipelines%2Forders%3Ftab%3Dtables";
+        let (code, head, _) = request(addr, "POST", "/ui/sign-in", &[], form).await;
+        assert_eq!(code, 303);
+        assert_eq!(
+            header_of(&head, "location"),
+            ["/ui/pipelines/orders?tab=tables"]
+        );
+        let set = header_of(&head, "set-cookie");
+        assert!(
+            set[0].starts_with("da_session=")
+                && set[0].ends_with(&format!(
+                    "; Path=/; Max-Age={}; HttpOnly; Secure; SameSite=Strict",
+                    (8 * HOUR + KEPT_AFTER).as_secs()
+                )),
+            "{set:?}"
+        );
+        let cookie = kept(&head);
+        let with = [("Cookie", cookie.as_str())];
+        let (code, _, page) = request(addr, "GET", "/ui/pipelines/orders", &with, "").await;
+        assert_eq!(code, 200);
+        assert!(
+            page.contains("You are <b>ops</b>, signed in as <b>admin</b>.")
+                && page.contains(r#"action="/ui/sign-out""#),
+            "{page}"
+        );
+        let (code, _, text) = request(addr, "GET", "/ui/session", &with, "").await;
+        assert_eq!(code, 200);
+        let v: Value = serde_json::from_str(&text).unwrap();
+        assert_eq!(
+            (&v["name"], &v["role"], &v["auth"]),
+            (&"ops".into(), &"admin".into(), &"basic".into())
+        );
+        assert!(v["csrf"].as_str().is_some_and(|c| c.len() == 43), "{v}");
+        assert!(
+            v["expires"].is_string() && v["signed_in"].is_string(),
+            "{v}"
+        );
+        // Signed in, the sign-in page goes on to `next`.
+        let (code, head, _) = request(addr, "GET", "/ui/sign-in?next=/ui/x", &with, "").await;
+        assert_eq!((code, header_of(&head, "location")), (303, vec!["/ui/x"]));
+    }
+
+    /// Mockup `02-sign-in.html`: a wrong password says how many attempts are left; after
+    /// [`SIGN_IN_ATTEMPTS`] the address waits [`SIGN_IN_WAIT`], the right password too.
+    #[tokio::test]
+    async fn wrong_passwords_leave_fewer_attempts_then_a_wait() {
+        let addr = node(&basic_api("s3cret"), &sessions(HOUR)).await;
+        for left in (1..SIGN_IN_ATTEMPTS).rev() {
+            let (code, _, page) =
+                request(addr, "POST", "/ui/sign-in", &[], "user=ops&password=nope").await;
+            assert_eq!(code, 401);
+            let s = if left == 1 { "" } else { "s" };
+            assert!(
+                page.contains(&format!(
+                    "Wrong user or password. {left} attempt{s} left before a 1 min wait."
+                )),
+                "{page}"
+            );
+            assert!(page.contains(r#"value="ops""#), "the user is kept: {page}");
+        }
+        let (code, _, page) =
+            request(addr, "POST", "/ui/sign-in", &[], "user=ops&password=nope").await;
+        assert_eq!(code, 401);
+        assert!(page.contains("Wait 1 min before trying again."), "{page}");
+        let (code, _, page) =
+            request(addr, "POST", "/ui/sign-in", &[], "user=ops&password=s3cret").await;
+        assert_eq!(code, 429, "{page}");
+        assert!(page.contains("Too many failed sign-ins"), "{page}");
+    }
+
+    #[test]
+    fn failures_are_counted_by_address_and_a_success_starts_again() {
+        let a: IpAddr = "10.0.0.1".parse().unwrap();
+        let b: IpAddr = "10.0.0.2".parse().unwrap();
+        let attempts = Attempts::default();
+        let t0 = Instant::now();
+        assert_eq!(attempts.failed(a, t0), SIGN_IN_ATTEMPTS - 1);
+        attempts.succeeded(a);
+        for _ in 0..SIGN_IN_ATTEMPTS {
+            attempts.failed(a, t0);
+        }
+        assert_eq!(attempts.check(a, t0), Err(SIGN_IN_WAIT));
+        assert_eq!(attempts.check(b, t0), Ok(()), "another address");
+        let later = t0 + SIGN_IN_WAIT;
+        assert_eq!(attempts.check(a, later), Ok(()));
+        assert_eq!(
+            attempts.failed(a, later),
+            SIGN_IN_ATTEMPTS - 1,
+            "the wait over, the count starts again"
+        );
+    }
+
+    /// Admin_Console §9: a session that no longer verifies is "Your session has ended", saying
+    /// why, with a way back to the page; captioned in words.
+    #[tokio::test]
+    async fn an_ended_session_says_why_and_leads_back() {
+        let addr = node(&basic_api("s3cret"), &sessions(HOUR)).await;
+        let old = session_cookie(&basic_session("s3cret", 2 * HOUR));
+        let with = [("Cookie", old.as_str())];
+        let (code, _, page) = request(addr, "GET", "/ui/nodes/node-b", &with, "").await;
+        assert_eq!(code, 401);
+        assert!(page.contains("Your session has ended"), "{page}");
+        assert!(page.contains("The session expired at "), "{page}");
+        assert!(page.contains("(1 h after sign-in)."), "{page}");
+        assert!(
+            page.contains(r#"href="/ui/sign-in?next=%2Fui%2Fnodes%2Fnode-b""#),
+            "{page}"
+        );
+        assert!(!page.contains("401"), "in words, not codes");
+        let (code, _, text) = request(addr, "GET", "/ui/session", &with, "").await;
+        assert_eq!(code, 401, "{text}");
+        let wrong_key = "da_session=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
+        let (_, _, page) = request(addr, "GET", "/ui/", &[("Cookie", wrong_key)], "").await;
+        assert!(page.contains("doesn&#39;t decrypt"), "{page}");
+    }
+
+    /// Admin_Console §9, `jwt`: no sign-in page and no session: "No valid token", no button; a
+    /// token whose roles map to nothing is "Forbidden", naming the roles it carried.
+    #[tokio::test]
+    async fn with_jwt_a_page_needs_a_valid_token() {
+        let idp = Issuer::start().await;
+        let addr = node(&idp.api("roles"), &enabled()).await;
+        let (code, _, page) = request(addr, "GET", "/ui/", &[], "").await;
+        assert_eq!(code, 401);
+        assert!(
+            page.contains("No valid token") && page.contains("a bearer token is required"),
+            "{page}"
+        );
+        assert!(!page.contains("Sign in"), "nothing to sign in to: {page}");
+        let (code, head, _) = request(addr, "GET", "/ui/sign-in?next=/ui/x", &[], "").await;
+        assert_eq!((code, header_of(&head, "location")), (303, vec!["/ui/x"]));
+        let token = format!("Bearer {}", idp.token(serde_json::json!(["viewer"])));
+        let (code, _, page) = request(
+            addr,
+            "GET",
+            "/ui/",
+            &[("Authorization", token.as_str())],
+            "",
+        )
+        .await;
+        assert_eq!(code, 200);
+        assert!(
+            page.contains("signed in as <b>viewer</b>") && !page.contains("Sign out"),
+            "{page}"
+        );
+        let token = format!(
+            "Bearer {}",
+            idp.token(serde_json::json!(["cdc-reader", "reporting"]))
+        );
+        let (code, _, page) = request(
+            addr,
+            "GET",
+            "/ui/",
+            &[("Authorization", token.as_str())],
+            "",
+        )
+        .await;
+        assert_eq!(code, 403);
+        assert!(
+            page.contains("Forbidden")
+                && page.contains("You are <b>alice</b>, and the token&#39;s roles (cdc-reader, reporting) name no role this node maps."),
+            "{page}"
+        );
+    }
+
+    /// Admin_Console §9, `none`: straight in as admin, and every page says so.
+    #[tokio::test]
+    async fn with_none_every_page_warns_there_is_no_authentication() {
+        let addr = node(&config().globals.api, &enabled()).await;
+        let (code, _, page) = request(addr, "GET", "/ui/overview", &[], "").await;
+        assert_eq!(code, 200);
+        assert!(
+            page.contains(
+                "This cluster has no authentication: anyone who can reach it is an admin."
+            ) && page.contains("You are <b>anonymous</b>, signed in as <b>admin</b>."),
+            "{page}"
+        );
+        let (code, head, _) = request(addr, "GET", "/ui/sign-in", &[], "").await;
+        assert_eq!((code, header_of(&head, "location")), (303, vec!["/ui/"]));
+    }
+
+    /// Admin_Console §9: a form another site posts (a sign-in to the attacker's account, a
+    /// sign-out) is refused; the console's own sign-out removes every session cookie.
+    #[tokio::test]
+    async fn a_form_another_site_posts_is_refused() {
+        let addr = node(&basic_api("s3cret"), &sessions(HOUR)).await;
+        let form = "user=ops&password=s3cret";
+        for from in [
+            [("Sec-Fetch-Site", "cross-site"), ("Origin", "http://t")],
+            [("Sec-Fetch-Site", "same-site"), ("Origin", "http://t")],
+            [("Origin", "https://evil.example"), ("X-Other", "")],
+            [("Origin", "null"), ("X-Other", "")],
+        ] {
+            let (code, _, page) = request(addr, "POST", "/ui/sign-in", &from, form).await;
+            assert_eq!(code, 403, "{from:?}");
+            assert!(page.contains("sent from another site"), "{page}");
+            let (code, _, _) = request(addr, "POST", "/ui/sign-out", &from, "").await;
+            assert_eq!(code, 403, "{from:?}");
+        }
+        let own = [("Sec-Fetch-Site", "same-origin"), ("Origin", "http://t")];
+        let (code, _, _) = request(addr, "POST", "/ui/sign-in", &own, form).await;
+        assert_eq!(code, 303);
+        let (code, _, _) =
+            request(addr, "POST", "/ui/sign-in", &[("Origin", "http://t")], form).await;
+        assert_eq!(code, 303, "Origin against Host");
+        let (code, head, _) = request(addr, "POST", "/ui/sign-out", &own, "").await;
+        assert_eq!(code, 303);
+        assert_eq!(header_of(&head, "location"), ["/ui/sign-in?signed_out"]);
+        let removed = header_of(&head, "set-cookie");
+        assert_eq!(removed.len(), crate::MAX_CHUNKS);
+        assert!(
+            removed.iter().all(|c| c.contains("Max-Age=0")),
+            "{removed:?}"
+        );
+        let (_, _, page) = request(addr, "GET", "/ui/sign-in?signed_out", &[], "").await;
+        assert!(page.contains("You signed out."), "{page}");
+    }
+
+    #[test]
+    fn next_is_always_a_console_page() {
+        for (asked, taken) in [
+            (
+                Some("/ui/pipelines/orders?tab=tables"),
+                "/ui/pipelines/orders?tab=tables",
+            ),
+            (None, "/ui/"),
+            (Some("https://evil.example/ui/"), "/ui/"),
+            (Some("//evil.example/ui/"), "/ui/"),
+            (Some("/ui//evil.example"), "/ui/"),
+            (Some("/ui/\\evil.example"), "/ui/"),
+            (Some("/v1/pipelines"), "/ui/"),
+            (Some("/ui/\r\nSet-Cookie: x"), "/ui/"),
+        ] {
+            assert_eq!(safe_next(asked), taken, "{asked:?}");
+        }
+    }
+
+    #[test]
+    fn pages_escape_what_they_show() {
+        let frame = Frame {
+            node: "<b>n</b>".into(),
+            open: false,
+        };
+        let page = pages::forbidden(&frame, "<script>x</script>", "it's \"so\" & so", false);
+        assert!(
+            page.contains("&lt;script&gt;x&lt;/script&gt;")
+                && page.contains("it&#39;s &quot;so&quot; &amp; so")
+                && page.contains("&lt;b&gt;n&lt;/b&gt;"),
+            "{page}"
+        );
+        assert!(!page.contains("<script>x"), "{page}");
+    }
+
+    #[tokio::test]
+    async fn the_stylesheet_is_served_and_nothing_else() {
+        let addr = node(&config().globals.api, &enabled()).await;
+        let (code, head, css) = request(addr, "GET", "/ui/assets/console.css", &[], "").await;
+        assert_eq!(code, 200);
+        assert_eq!(
+            header_of(&head, "content-type"),
+            ["text/css; charset=utf-8"]
+        );
+        assert!(css.contains("prefers-color-scheme: dark"), "both themes");
+        for other in ["/ui/assets/other.js", "/ui/assets/..%2Fsrc%2Flib.rs"] {
+            assert_eq!(request(addr, "GET", other, &[], "").await.0, 404, "{other}");
+        }
+    }
+}
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 15167ff..53ae6da 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -14,6 +14,7 @@ mod actions;
 mod auth;
 mod cluster;
 mod config;
+mod console;
 mod events;
 mod follow;
 mod generations;
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api --lib -- console:: auth::`
Expected: the API's tests don't compile: the new module's find no `SIGN_IN_ATTEMPTS`, `SIGN_IN_WAIT`, `KEPT_AFTER`, `Attempts`, `safe_next`, `Frame` or `pages` (nor the imports the code above them brings: `Duration`, `Instant`, `IpAddr`).

- [ ] **Step 3: Implement**

`pages.rs` renders each page from the mockup (`02-sign-in.html`) with every value escaped;
`assets/console.css` is the mockup's stylesheet, served by `include_str!`. The server serves the
router with `into_make_service_with_connect_info::<Client>()` on both listeners. Task 1's
`expect(dead_code)` marks on what only the sign-in uses go.

`crates/data-avalanche-api/assets/console.css`:

````diff
diff --git a/crates/data-avalanche-api/assets/console.css b/crates/data-avalanche-api/assets/console.css
new file mode 100644
index 0000000..d009db0
--- /dev/null
+++ b/crates/data-avalanche-api/assets/console.css
@@ -0,0 +1,125 @@
+/* The console's sign-in and error pages (Admin_Console §9; mockup 02-sign-in.html). */
+:root {
+  --bg-primary: #f5f5f7;
+  --bg-secondary: #ffffff;
+  --bg-tertiary: #e5e5e7;
+  --border: #d1d1d6;
+  --text-primary: #1d1d1f;
+  --text-secondary: #6e6e73;
+  --accent: #0071e3;
+  --error-bg: rgba(255, 59, 48, 0.12);
+  --error-text: #c0302a;
+  --info-bg: rgba(10, 132, 255, 0.1);
+  --warn-bg: rgba(255, 149, 0, 0.16);
+  --warn-text: #9a5a00;
+}
+
+@media (prefers-color-scheme: dark) {
+  :root {
+    --bg-primary: #1d1d1f;
+    --bg-secondary: #2d2d2f;
+    --bg-tertiary: #3d3d3f;
+    --border: #424245;
+    --text-primary: #f5f5f7;
+    --text-secondary: #a1a1a6;
+    --accent: #0a84ff;
+    --error-text: #ff6961;
+    --warn-text: #ffb340;
+  }
+}
+
+* { box-sizing: border-box; }
+
+body {
+  margin: 0;
+  min-height: 100vh;
+  font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
+  background: var(--bg-primary);
+  color: var(--text-primary);
+}
+
+.warnband {
+  background: var(--warn-bg);
+  color: var(--warn-text);
+  padding: 8px 16px;
+  font-size: 13px;
+  border-bottom: 1px solid var(--border);
+}
+
+main {
+  display: flex;
+  justify-content: center;
+  padding: 48px 16px;
+}
+
+.card {
+  width: 100%;
+  max-width: 360px;
+  background: var(--bg-secondary);
+  border: 1px solid var(--border);
+  border-radius: 10px;
+  padding: 20px 22px;
+}
+
+.brand { font-size: 16px; font-weight: 600; }
+.brand small { font-weight: 400; color: var(--text-secondary); font-size: 13px; margin-left: 6px; }
+.title { font-size: 18px; font-weight: 600; margin: 6px 0 4px; }
+.muted { color: var(--text-secondary); }
+p { margin: 8px 0; }
+code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.92em; }
+
+label { display: block; font-size: 13px; color: var(--text-secondary); margin: 12px 0 4px; }
+
+input[type="text"],
+input[type="password"] {
+  width: 100%;
+  border: 1px solid var(--border);
+  border-radius: 6px;
+  padding: 7px 9px;
+  background: var(--bg-primary);
+  color: var(--text-primary);
+  font: inherit;
+}
+
+input:focus-visible,
+.btn:focus-visible,
+a:focus-visible {
+  outline: 2px solid var(--accent);
+  outline-offset: 2px;
+}
+
+.btn {
+  display: block;
+  width: 100%;
+  text-align: center;
+  border: 0;
+  border-radius: 6px;
+  padding: 8px 10px;
+  margin-top: 16px;
+  font: inherit;
+  background: var(--accent);
+  color: #fff;
+  text-decoration: none;
+  cursor: pointer;
+}
+
+.btn.sec { background: var(--bg-secondary); color: var(--text-primary); border: 1px solid var(--border); }
+
+.msg { border-radius: 6px; padding: 8px 10px; font-size: 13px; margin-top: 12px; }
+.msg.err { background: var(--error-bg); color: var(--error-text); }
+.msg.info { background: var(--info-bg); }
+
+.count { height: 4px; background: var(--bg-tertiary); border-radius: 2px; overflow: hidden; margin-top: 12px; }
+.count i { display: block; height: 100%; width: 100%; background: var(--accent); transition: width 1s linear; }
+
+a { color: var(--accent); }
+
+.foot {
+  font-size: 12px;
+  color: var(--text-secondary);
+  margin-top: 16px;
+  border-top: 1px solid var(--border);
+  padding-top: 8px;
+}
+
+[hidden] { display: none !important; }
````

`crates/data-avalanche-api/src/auth.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/auth.rs b/crates/data-avalanche-api/src/auth.rs
index eff3711..863a5b9 100644
--- a/crates/data-avalanche-api/src/auth.rs
+++ b/crates/data-avalanche-api/src/auth.rs
@@ -29,6 +29,7 @@ use serde_json::Value;
 use subtle::ConstantTimeEq;
 use tokio::sync::watch;
 
+use crate::console::Attempts;
 use crate::session::{By, Sealer, Session, Unsealed, session_cookies};
 
 /// A JWKS fetch, or a discovery, takes at most this long.
@@ -154,6 +155,8 @@ struct Settings {
     /// The console's session key and `session_ttl`: set when the console is enabled with `auth`
     /// `basic` or `oauth2` and a key (V-135).
     sessions: Option<(Secret, Duration)>,
+    /// `globals.console.enabled`.
+    console: bool,
 }
 
 impl Settings {
@@ -170,6 +173,7 @@ impl Settings {
             role_mapping: api.role_mapping.clone(),
             username_claim: console.username_claim.clone(),
             sessions,
+            console: console.enabled,
         }
     }
 }
@@ -214,11 +218,22 @@ struct Current {
     sessions: Option<Arc<Sessions>>,
 }
 
+/// What the console's own routes (`/ui/`) read of the settings: how people sign in, and the
+/// sessions that keeps them signed in.
+#[derive(Debug, Clone)]
+pub(crate) struct SignIn {
+    pub auth: ApiAuth,
+    pub sessions: Option<Arc<Sessions>>,
+    pub credentials: Option<Credentials>,
+}
+
 /// The node's authentication, swapped by a reload.
 #[derive(Debug)]
 pub struct Auth {
     current: RwLock<Current>,
     timing: Timing,
+    /// The failed basic sign-ins of each address (Admin_Console §9).
+    pub(crate) attempts: Attempts,
 }
 
 impl Auth {
@@ -261,6 +276,7 @@ impl Auth {
                 settings,
             }),
             timing,
+            attempts: Attempts::default(),
         }
     }
 
@@ -282,6 +298,17 @@ impl Auth {
         }
     }
 
+    /// How people sign in to the console, while it is enabled.
+    pub(crate) fn sign_in(&self) -> Option<SignIn> {
+        let current = self.current.read().unwrap_or_else(PoisonError::into_inner);
+        let s = &current.settings;
+        s.console.then(|| SignIn {
+            auth: s.auth,
+            sessions: current.sessions.clone(),
+            credentials: s.credentials.clone(),
+        })
+    }
+
     /// The caller of a request with these headers.
     pub(crate) async fn verify(&self, headers: &HeaderMap) -> Result<Principal, Denied> {
         self.verify_caller(headers).await.map(|v| v.principal)
@@ -473,6 +500,12 @@ fn same(given: &str, expected: &str) -> subtle::Choice {
         .ct_eq(blake3::hash(expected.as_bytes()).as_bytes())
 }
 
+/// Whether `user` and `password` are the configured ones, both compared in full whichever
+/// differs.
+pub(crate) fn basic_matches(c: &Credentials, user: &str, password: &str) -> bool {
+    bool::from(same(user, c.user()) & same(password, c.password().expose()))
+}
+
 fn basic(c: &Credentials, headers: &HeaderMap) -> Result<Principal, Denied> {
     let given = credential(headers, "Basic")
         .ok_or_else(|| unauthenticated(BASIC, "basic credentials are required"))?;
@@ -484,10 +517,7 @@ fn basic(c: &Credentials, headers: &HeaderMap) -> Result<Principal, Denied> {
     let (user, password) = decoded
         .split_once(':')
         .ok_or_else(|| unauthenticated(BASIC, "the basic credentials don't decode"))?;
-    // Both compared in full, whichever differs.
-    let user_ok = same(user, c.user());
-    let password_ok = same(password, c.password().expose());
-    if bool::from(user_ok & password_ok) {
+    if basic_matches(c, user, password) {
         Ok(Principal {
             name: user.to_owned(),
             role: Role::Admin,
@@ -557,14 +587,16 @@ impl Verifier {
             .get("exp")
             .and_then(Value::as_i64)
             .and_then(|exp| DateTime::from_timestamp(exp, 0));
-        let Some(role) = roles(&claims, &self.roles_claim)
-            .iter()
-            .filter_map(|r| map(&self.mapping, r))
-            .max()
-        else {
+        let carried = roles(&claims, &self.roles_claim);
+        let Some(role) = carried.iter().filter_map(|r| map(&self.mapping, r)).max() else {
+            let carried = if carried.is_empty() {
+                "none".to_owned()
+            } else {
+                carried.join(", ")
+            };
             return Err(Denied::Forbidden {
                 why: format!(
-                    "the token's {} name no role this node maps",
+                    "the token's {} ({carried}) name no role this node maps",
                     self.roles_claim
                 ),
                 name,
````

`crates/data-avalanche-api/src/console.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/console.rs b/crates/data-avalanche-api/src/console.rs
index 30368c3..1a0833e 100644
--- a/crates/data-avalanche-api/src/console.rs
+++ b/crates/data-avalanche-api/src/console.rs
@@ -1,3 +1,470 @@
+//! The console's own routes under `/ui/` (Admin_Console §9, §10): the sign-in and the sign-out,
+//! the session as the console's app reads it, and the pages a browser asks for. They are served
+//! while `globals.console.enabled`, read at each request, so a reload turns them on or off: `404`
+//! otherwise. Every answer forbids framing and runs no inline script or style.
+//!
+//! A page asked for without credentials goes to the sign-in (`basic`, `oauth2`); with a session
+//! that no longer verifies it is "Your session has ended" (401), saying why; with `jwt`, "No valid
+//! token" (401); with a role the node doesn't map, "Forbidden" (403).
+
+use std::collections::{BTreeMap, HashMap};
+use std::net::{IpAddr, Ipv4Addr};
+use std::sync::{Mutex, PoisonError};
+use std::time::{Duration, Instant};
+
+use axum::body::Bytes;
+use axum::extract::{ConnectInfo, Path, RawQuery, Request, State};
+use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
+use axum::middleware::{Next, from_fn_with_state};
+use axum::response::{IntoResponse, Response};
+use axum::routing::{get, post};
+use axum::{Extension, Router};
+use chrono::{SecondsFormat, Utc};
+use data_avalanche_config::ApiAuth;
+use serde_json::json;
+
+use crate::auth::{Denied, SignIn, Verified};
+use crate::pages::{self, Frame};
+use crate::session::{By, KEPT_AFTER, Session, sealed_value};
+use crate::{Api, body};
+
+/// Failed basic sign-ins in a row from one address before it waits.
+pub const SIGN_IN_ATTEMPTS: u32 = 5;
+/// How long an address waits after [`SIGN_IN_ATTEMPTS`] failed sign-ins.
+pub const SIGN_IN_WAIT: Duration = Duration::from_secs(60);
+/// At most this many addresses' failures are kept; past it, those not waiting are forgotten.
+const ADDRESSES_KEPT: usize = 10_000;
+
+/// The Content-Security-Policy of every `/ui/` answer: the console's own scripts, styles and
+/// connections, nothing inline, no framing. `form-action` adds the provider's end-session
+/// origin, where the sign-out goes on with `oauth2`.
+fn policy(form_action: &str) -> String {
+    format!(
+        "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; form-action 'self'{form_action}; frame-ancestors 'none'; base-uri 'none'"
+    )
+}
+
+/// The console's routes.
+pub(crate) fn routes(api: &Api) -> Router<Api> {
+    Router::new()
+        .route("/ui", get(|| async { see_other("/ui/") }))
+        .route("/ui/", get(page))
+        .route("/ui/{*page}", get(page))
+        .route("/ui/sign-in", get(sign_in_page).post(sign_in))
+        .route("/ui/sign-out", post(sign_out))
+        .route("/ui/session", get(session))
+        .route("/ui/assets/{file}", get(asset))
+        .layer(from_fn_with_state(api.clone(), guard))
+}
+
+/// Serves `/ui/` while the console is enabled, and sets every answer's security headers.
+async fn guard(State(api): State<Api>, req: Request, next: Next) -> Response {
+    if api.auth.sign_in().is_none() {
+        return (
+            StatusCode::NOT_FOUND,
+            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
+            "The console is off on this cluster: globals.console.enabled.\n",
+        )
+            .into_response();
+    }
+    let mut r = next.run(req).await;
+    let h = r.headers_mut();
+    let csp = policy("");
+    if let Ok(v) = HeaderValue::from_str(&csp) {
+        h.insert(header::CONTENT_SECURITY_POLICY, v);
+    }
+    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
+    h.insert(
+        header::X_CONTENT_TYPE_OPTIONS,
+        HeaderValue::from_static("nosniff"),
+    );
+    h.insert(
+        header::REFERRER_POLICY,
+        HeaderValue::from_static("no-referrer"),
+    );
+    h.entry(header::CACHE_CONTROL)
+        .or_insert(HeaderValue::from_static("no-store"));
+    r
+}
+
+/// `303 See Other` to `to`.
+fn see_other(to: &str) -> Response {
+    (StatusCode::SEE_OTHER, [(header::LOCATION, to)]).into_response()
+}
+
+/// An HTML page with `code`.
+fn html(code: StatusCode, page: String) -> Response {
+    (
+        code,
+        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
+        page,
+    )
+        .into_response()
+}
+
+/// `next` when it is a page of the console's own: a path under `/ui/`, never another site's.
+pub(crate) fn safe_next(next: Option<&str>) -> String {
+    match next {
+        Some(n)
+            if n.starts_with("/ui/")
+                && !n.contains("//")
+                && !n.contains('\\')
+                && !n.chars().any(char::is_control) =>
+        {
+            n.to_owned()
+        }
+        _ => "/ui/".to_owned(),
+    }
+}
+
+/// The query's parameters, the last of each name taken.
+fn params(raw: Option<&str>) -> BTreeMap<String, String> {
+    url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
+        .into_owned()
+        .collect()
+}
+
+fn frame(api: &Api, sign_in: &SignIn) -> Frame {
+    Frame {
+        node: api
+            .node
+            .get()
+            .map_or_else(String::new, |n| n.members.borrow().me.clone()),
+        open: sign_in.auth == ApiAuth::None,
+    }
+}
+
+/// A page under `/ui/` (the app's, from M15): the landing page for a caller who verifies, the
+/// sign-in for one who brought nothing, else why not.
+async fn page(State(api): State<Api>, uri: Uri, headers: HeaderMap) -> Response {
+    let Some(sign_in) = api.auth.sign_in() else {
+        return StatusCode::NOT_FOUND.into_response();
+    };
+    let frame = frame(&api, &sign_in);
+    let here = safe_next(uri.path_and_query().map(|p| p.as_str()));
+    match api.auth.verify_caller(&headers).await {
+        Ok(Verified { principal, session }) => html(
+            StatusCode::OK,
+            pages::signed_in(
+                &frame,
+                &principal.name,
+                &principal.role.to_string(),
+                session.is_some(),
+            ),
+        ),
+        Err(denied) => refused(&frame, &sign_in, &headers, &here, denied),
+    }
+}
+
+/// Why a page isn't shown: the sign-in for one who brought no credentials (`basic`, `oauth2`),
+/// else its page.
+fn refused(
+    frame: &Frame,
+    sign_in: &SignIn,
+    headers: &HeaderMap,
+    here: &str,
+    denied: Denied,
+) -> Response {
+    match denied {
+        Denied::Unauthenticated { why, .. } => match sign_in.auth {
+            ApiAuth::Jwt => html(StatusCode::UNAUTHORIZED, pages::no_token(frame, &why)),
+            _ if sealed_value(headers).is_none()
+                && !headers.contains_key(header::AUTHORIZATION) =>
+            {
+                see_other(&format!("/ui/sign-in?next={}", pages::encode(here)))
+            }
+            _ => html(StatusCode::UNAUTHORIZED, pages::ended(frame, &why, here)),
+        },
+        Denied::Forbidden { name, why } => html(
+            StatusCode::FORBIDDEN,
+            pages::forbidden(frame, &name, &why, sealed_value(headers).is_some()),
+        ),
+        Denied::Unavailable(why) => html(
+            StatusCode::SERVICE_UNAVAILABLE,
+            pages::unavailable(frame, &why),
+        ),
+    }
+}
+
+/// `GET /ui/sign-in`: the form (`basic`); a caller already signed in goes on to `next`.
+async fn sign_in_page(
+    State(api): State<Api>,
+    RawQuery(raw): RawQuery,
+    headers: HeaderMap,
+) -> Response {
+    let Some(sign_in) = api.auth.sign_in() else {
+        return StatusCode::NOT_FOUND.into_response();
+    };
+    let q = params(raw.as_deref());
+    let next = safe_next(q.get("next").map(String::as_str));
+    match sign_in.auth {
+        ApiAuth::None | ApiAuth::Jwt => return see_other(&next),
+        ApiAuth::Basic | ApiAuth::Oauth2 => {}
+    }
+    if let Ok(Verified {
+        session: Some(_), ..
+    }) = api.auth.verify_caller(&headers).await
+    {
+        return see_other(&next);
+    }
+    let frame = frame(&api, &sign_in);
+    match sign_in.auth {
+        ApiAuth::Basic => {
+            let info = q.contains_key("signed_out").then_some("You signed out.");
+            html(
+                StatusCode::OK,
+                pages::basic_sign_in(&frame, &next, "", None, info),
+            )
+        }
+        _ => StatusCode::NOT_FOUND.into_response(),
+    }
+}
+
+/// Whether a form posted to `/ui/` came from the console's own pages, as the browser says
+/// (`Sec-Fetch-Site`, else `Origin` against `Host`); a client that says neither (`curl`) is
+/// taken at its word.
+fn same_origin(headers: &HeaderMap) -> bool {
+    if let Some(site) = headers.get("sec-fetch-site") {
+        return site == "same-origin" || site == "none";
+    }
+    let Some(origin) = headers.get(header::ORIGIN) else {
+        return true;
+    };
+    let host = headers.get(header::HOST).and_then(|h| h.to_str().ok());
+    let origin = origin.to_str().ok().and_then(|o| url::Url::parse(o).ok());
+    match (origin, host) {
+        (Some(o), Some(host)) => {
+            let authority = match o.port() {
+                Some(port) => format!("{}:{port}", o.host_str().unwrap_or_default()),
+                None => o.host_str().unwrap_or_default().to_owned(),
+            };
+            authority.eq_ignore_ascii_case(host)
+        }
+        _ => false,
+    }
+}
+
+/// The client's address, as the connection says (a proxy's own, behind one).
+fn client(info: Option<Extension<ConnectInfo<crate::server::Client>>>) -> IpAddr {
+    info.map_or(
+        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
+        |Extension(ConnectInfo(c))| c.0.ip(),
+    )
+}
+
+/// `POST /ui/sign-in` (`basic`): the user and password checked as the API checks them; right,
+/// a session and on to `next`; wrong, the form again, with how many attempts are left before the
+/// address waits [`SIGN_IN_WAIT`].
+async fn sign_in(
+    State(api): State<Api>,
+    info: Option<Extension<ConnectInfo<crate::server::Client>>>,
+    headers: HeaderMap,
+    form: Bytes,
+) -> Response {
+    let Some(sign_in) = api.auth.sign_in() else {
+        return StatusCode::NOT_FOUND.into_response();
+    };
+    let frame = frame(&api, &sign_in);
+    let (ApiAuth::Basic, Some(credentials), Some(sessions)) =
+        (sign_in.auth, &sign_in.credentials, &sign_in.sessions)
+    else {
+        return StatusCode::NOT_FOUND.into_response();
+    };
+    if !same_origin(&headers) {
+        return html(StatusCode::FORBIDDEN, pages::cross_site(&frame));
+    }
+    let form: BTreeMap<String, String> = url::form_urlencoded::parse(&form).into_owned().collect();
+    let field = |name: &str| form.get(name).map_or("", String::as_str);
+    let next = safe_next(form.get("next").map(String::as_str));
+    let from = client(info);
+    let now = Instant::now();
+    if let Err(wait) = api.auth.attempts.check(from, now) {
+        return html(
+            StatusCode::TOO_MANY_REQUESTS,
+            pages::waiting(&frame, &next, wait.as_secs().max(1)),
+        );
+    }
+    if !crate::auth::basic_matches(credentials, field("user"), field("password")) {
+        let left = api.auth.attempts.failed(from, now);
+        let error = if left == 0 {
+            format!(
+                "Wrong user or password. Wait {} before trying again.",
+                crate::auth::span(SIGN_IN_WAIT)
+            )
+        } else {
+            format!(
+                "Wrong user or password. {left} attempt{s} left before a {} wait.",
+                crate::auth::span(SIGN_IN_WAIT),
+                s = if left == 1 { "" } else { "s" },
+            )
+        };
+        return html(
+            StatusCode::UNAUTHORIZED,
+            pages::basic_sign_in(&frame, &next, field("user"), Some(&error), None),
+        );
+    }
+    api.auth.attempts.succeeded(from);
+    let session = Session::new(
+        By::Basic {
+            user: field("user").to_owned(),
+            fingerprint: sessions.sealer.fingerprint(field("password")),
+        },
+        Utc::now().timestamp(),
+    );
+    started(&session, sessions, &next)
+}
+
+/// A session started: its cookies, and on to `next`.
+pub(crate) fn started(session: &Session, sessions: &crate::auth::Sessions, next: &str) -> Response {
+    let max_age = (sessions.ttl + KEPT_AFTER).as_secs();
+    let cookies = match session.cookies(&sessions.sealer, max_age) {
+        Ok(c) => c,
+        Err(e) => {
+            return body(
+                StatusCode::INTERNAL_SERVER_ERROR,
+                &json!({ "error": e.to_string() }),
+            );
+        }
+    };
+    let mut r = see_other(next);
+    for c in cookies {
+        if let Ok(v) = HeaderValue::from_str(&c) {
+            r.headers_mut().append(header::SET_COOKIE, v);
+        }
+    }
+    r
+}
+
+/// `POST /ui/sign-out`: the session's cookies removed, then the sign-in. Only from the
+/// console's own pages: a sign-out another site posts is refused.
+async fn sign_out(State(api): State<Api>, headers: HeaderMap) -> Response {
+    let Some(sign_in) = api.auth.sign_in() else {
+        return StatusCode::NOT_FOUND.into_response();
+    };
+    if !same_origin(&headers) {
+        return html(
+            StatusCode::FORBIDDEN,
+            pages::cross_site(&frame(&api, &sign_in)),
+        );
+    }
+    let mut r = see_other("/ui/sign-in?signed_out");
+    for c in Session::removed() {
+        if let Ok(v) = HeaderValue::from_str(&c) {
+            r.headers_mut().append(header::SET_COOKIE, v);
+        }
+    }
+    r
+}
+
+/// `GET /ui/session`: who the caller is, as the console's app reads it: name, role, how they
+/// signed in, until when, and the session's anti-forgery token.
+async fn session(State(api): State<Api>, headers: HeaderMap) -> Response {
+    let Some(sign_in) = api.auth.sign_in() else {
+        return StatusCode::NOT_FOUND.into_response();
+    };
+    match api.auth.verify_caller(&headers).await {
+        Ok(Verified { principal, session }) => body(
+            StatusCode::OK,
+            &json!({
+                "name": principal.name,
+                "role": principal.role,
+                "auth": sign_in.auth,
+                "expires": principal.expires.map(|e| e.to_rfc3339_opts(SecondsFormat::Secs, true)),
+                "signed_in": session.as_ref().and_then(|s| chrono::DateTime::from_timestamp(s.signed_in, 0)).map(|t| t.to_rfc3339_opts(SecondsFormat::Secs, true)),
+                "csrf": session.map(|s| s.csrf),
+            }),
+        ),
+        Err(Denied::Unauthenticated { why, .. }) => {
+            body(StatusCode::UNAUTHORIZED, &json!({ "error": why }))
+        }
+        Err(Denied::Forbidden { name, why }) => body(
+            StatusCode::FORBIDDEN,
+            &json!({ "error": why, "name": name }),
+        ),
+        Err(Denied::Unavailable(why)) => {
+            body(StatusCode::SERVICE_UNAVAILABLE, &json!({ "error": why }))
+        }
+    }
+}
+
+/// The pages' stylesheet and scripts, built into the binary.
+async fn asset(Path(file): Path<String>) -> Response {
+    let (text, kind) = match file.as_str() {
+        "console.css" => (
+            include_str!("../assets/console.css"),
+            "text/css; charset=utf-8",
+        ),
+        _ => return StatusCode::NOT_FOUND.into_response(),
+    };
+    (
+        [
+            (header::CONTENT_TYPE, kind),
+            (header::CACHE_CONTROL, "no-cache"),
+        ],
+        text,
+    )
+        .into_response()
+}
+
+/// The failed basic sign-ins of each address: [`SIGN_IN_ATTEMPTS`] in a row, and it waits
+/// [`SIGN_IN_WAIT`]; a sign-in that succeeds starts the count again. Kept by each node for
+/// itself, in memory.
+#[derive(Debug, Default)]
+pub(crate) struct Attempts(Mutex<HashMap<IpAddr, Failures>>);
+
+#[derive(Debug, Clone, Copy)]
+struct Failures {
+    count: u32,
+    last: Instant,
+    until: Option<Instant>,
+}
+
+impl Attempts {
+    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<IpAddr, Failures>> {
+        self.0.lock().unwrap_or_else(PoisonError::into_inner)
+    }
+
+    /// `Err` with the wait left while `from` waits; a wait over starts the count again.
+    pub(crate) fn check(&self, from: IpAddr, now: Instant) -> Result<(), Duration> {
+        let mut all = self.lock();
+        match all.get(&from).and_then(|f| f.until) {
+            Some(until) if until > now => Err(until - now),
+            Some(_) => {
+                all.remove(&from);
+                Ok(())
+            }
+            None => Ok(()),
+        }
+    }
+
+    /// A failed sign-in from `from`: the attempts left before it waits (0: it waits now).
+    pub(crate) fn failed(&self, from: IpAddr, now: Instant) -> u32 {
+        let mut all = self.lock();
+        if all.len() >= ADDRESSES_KEPT && !all.contains_key(&from) {
+            all.retain(|_, f| {
+                f.until.is_some_and(|u| u > now) || now.duration_since(f.last) < SIGN_IN_WAIT
+            });
+        }
+        let f = all.entry(from).or_insert(Failures {
+            count: 0,
+            last: now,
+            until: None,
+        });
+        f.count += 1;
+        f.last = now;
+        let left = SIGN_IN_ATTEMPTS.saturating_sub(f.count);
+        if left == 0 {
+            f.until = Some(now + SIGN_IN_WAIT);
+        }
+        left
+    }
+
+    /// A sign-in from `from` that succeeded.
+    pub(crate) fn succeeded(&self, from: IpAddr) {
+        self.lock().remove(&from);
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use std::net::SocketAddr;
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 53ae6da..b145c63 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -23,6 +23,7 @@ mod limit;
 mod live;
 mod nodes;
 mod openapi;
+mod pages;
 mod peers;
 mod pipelines;
 mod prometheus;
@@ -61,6 +62,7 @@ pub use auth::{
     Auth, CSRF_HEADER, FETCH_TIMEOUT, KEYS_MAX_AGE, MAX_KEY_DOCUMENT, Principal, REFETCH_AFTER,
 };
 pub use cluster::{Control, Stop};
+pub use console::{SIGN_IN_ATTEMPTS, SIGN_IN_WAIT};
 pub use follow::{FOLLOW_SILENCE, MAX_FRAME};
 pub use history::{MAX_POINTS, MAX_SPAN};
 pub use limit::{RateLimit, VALIDATE_BURST, VALIDATE_PER_SECOND};
@@ -893,6 +895,7 @@ pub fn router(api: Api) -> Router {
         );
     }
     router
+        .merge(console::routes(&api))
         .layer(from_fn_with_state(api.clone(), misdirected))
         .with_state(api)
 }
````

`crates/data-avalanche-api/src/pages.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/pages.rs b/crates/data-avalanche-api/src/pages.rs
new file mode 100644
index 0000000..660f2ea
--- /dev/null
+++ b/crates/data-avalanche-api/src/pages.rs
@@ -0,0 +1,202 @@
+//! The console's own pages (Admin_Console §9; mockup `02-sign-in.html`), rendered by the server:
+//! the sign-in, the 401 and 403 pages, and the landing page until the console's app is built
+//! (M15). They are captioned in words, not status codes. Each loads `/ui/assets/console.css`, and
+//! the oauth2 sign-in `/ui/assets/sign-in.js`: the Content-Security-Policy runs nothing inline.
+
+use std::fmt::Write as _;
+
+/// What every page shows around its card: the node that served it, and, with `auth: none`, the
+/// banner that says the cluster has no authentication.
+#[derive(Debug, Clone)]
+pub(crate) struct Frame {
+    pub node: String,
+    pub open: bool,
+}
+
+/// `s`, safe in HTML text and in a quoted attribute.
+pub(crate) fn escape(s: &str) -> String {
+    let mut out = String::with_capacity(s.len());
+    for c in s.chars() {
+        match c {
+            '&' => out.push_str("&amp;"),
+            '<' => out.push_str("&lt;"),
+            '>' => out.push_str("&gt;"),
+            '"' => out.push_str("&quot;"),
+            '\'' => out.push_str("&#39;"),
+            c => out.push(c),
+        }
+    }
+    out
+}
+
+/// A whole page: `title` in the browser's tab, `card` (HTML) in the middle, `script` loaded
+/// after it when set.
+fn page(frame: &Frame, title: &str, card: &str, script: Option<&str>) -> String {
+    let banner = if frame.open {
+        r#"<div class="warnband" role="alert">⚠ This cluster has no authentication: anyone who can reach it is an admin. Set <code>globals.api.auth</code>.</div>"#
+    } else {
+        ""
+    };
+    let script = script.map_or(String::new(), |src| {
+        format!(r#"<script src="{src}"></script>"#)
+    });
+    format!(
+        r#"<!doctype html>
+<html lang="en">
+<head>
+<meta charset="utf-8">
+<meta name="viewport" content="width=device-width, initial-scale=1">
+<title>{title} · data-avalanche</title>
+<link rel="stylesheet" href="/ui/assets/console.css">
+</head>
+<body>
+{banner}<main><div class="card">
+<div class="brand">data-avalanche<small>console</small></div>
+{card}
+<div class="foot">{node} · {version}</div>
+</div></main>
+{script}</body>
+</html>
+"#,
+        title = escape(title),
+        node = escape(&frame.node),
+        version = env!("CARGO_PKG_VERSION"),
+    )
+}
+
+/// The `basic` sign-in: a form, not the browser's popup, so signing out works. `error` and
+/// `info` are shown above the button.
+pub(crate) fn basic_sign_in(
+    frame: &Frame,
+    next: &str,
+    user: &str,
+    error: Option<&str>,
+    info: Option<&str>,
+) -> String {
+    let mut card = format!(
+        r#"<div class="muted">Sign in to the cluster</div>
+<form method="post" action="/ui/sign-in">
+<input type="hidden" name="next" value="{next}">
+<label for="user">User</label>
+<input type="text" id="user" name="user" value="{user}" autocomplete="username" required autofocus>
+<label for="password">Password</label>
+<input type="password" id="password" name="password" autocomplete="current-password" required>
+"#,
+        next = escape(next),
+        user = escape(user),
+    );
+    if let Some(e) = error {
+        let _ = writeln!(
+            card,
+            r#"<div class="msg err" role="alert">{}</div>"#,
+            escape(e)
+        );
+    }
+    if let Some(i) = info {
+        let _ = writeln!(card, r#"<div class="msg info">{}</div>"#, escape(i));
+    }
+    card.push_str("<button class=\"btn\" type=\"submit\">Sign in</button>\n</form>");
+    page(frame, "Sign in", &card, None)
+}
+
+/// 401 with a session (`basic`, `oauth2`): it ended, why, and a way back to `next`.
+pub(crate) fn ended(frame: &Frame, why: &str, next: &str) -> String {
+    let card = format!(
+        r#"<div class="title">Your session has ended</div>
+<p>{why}.</p>
+<p class="muted">Sign in again to go back to <code>{next}</code>.</p>
+<a class="btn" href="/ui/sign-in?next={encoded}">Sign in again</a>"#,
+        why = escape(&capitalized(why)),
+        next = escape(next),
+        encoded = escape(&encode(next)),
+    );
+    page(frame, "Session ended", &card, None)
+}
+
+/// 401 with `jwt`: there is nothing to sign in to; the proxy in front sends the token.
+pub(crate) fn no_token(frame: &Frame, why: &str) -> String {
+    let card = format!(
+        r#"<div class="title">No valid token</div>
+<p>This console expects a bearer token on every request, from the proxy in front of it: {why}.</p>
+<p class="muted">Reload once the proxy sends a token.</p>"#,
+        why = escape(why),
+    );
+    page(frame, "No valid token", &card, None)
+}
+
+/// 403: who the caller is, and why that isn't enough; with a session, a way out.
+pub(crate) fn forbidden(frame: &Frame, name: &str, why: &str, session: bool) -> String {
+    let mut card = format!(
+        r#"<div class="title">Forbidden</div>
+<p>You are <b>{name}</b>, and {why}.</p>
+<p class="muted">Ask an admin to map one of your roles through <code>globals.api.role_mapping</code>.</p>"#,
+        name = escape(name),
+        why = escape(why),
+    );
+    if session {
+        card.push_str(SIGN_OUT);
+    }
+    page(frame, "Forbidden", &card, None)
+}
+
+/// 503: nobody can be verified right now.
+pub(crate) fn unavailable(frame: &Frame, why: &str) -> String {
+    let card = format!(
+        r#"<div class="title">Signing in isn't possible right now</div>
+<p>{why}.</p>
+<p class="muted">Try again in a moment.</p>"#,
+        why = escape(&capitalized(why)),
+    );
+    page(frame, "Unavailable", &card, None)
+}
+
+/// 429: this address failed to sign in too often; it waits `seconds`.
+pub(crate) fn waiting(frame: &Frame, next: &str, seconds: u64) -> String {
+    let card = format!(
+        r#"<div class="title">Too many failed sign-ins</div>
+<p>Too many wrong passwords came from your address. Try again in {seconds} s.</p>
+<a class="btn sec" href="/ui/sign-in?next={encoded}">Back to the sign-in</a>"#,
+        encoded = escape(&encode(next)),
+    );
+    page(frame, "Wait", &card, None)
+}
+
+/// 403: a form posted from another site.
+pub(crate) fn cross_site(frame: &Frame) -> String {
+    let card = r#"<div class="title">Forbidden</div>
+<p>This form was sent from another site. Sign in from the console's own page.</p>
+<a class="btn sec" href="/ui/sign-in">Go to the sign-in</a>"#;
+    page(frame, "Forbidden", card, None)
+}
+
+/// The sign-out form: a session's way out.
+const SIGN_OUT: &str = r#"
+<form method="post" action="/ui/sign-out"><button class="btn sec" type="submit">Sign out</button></form>"#;
+
+/// The landing page, until the console's app is built (M15): who is signed in, and, with a
+/// session, a way out.
+pub(crate) fn signed_in(frame: &Frame, name: &str, role: &str, session: bool) -> String {
+    let mut card = format!(
+        r#"<div class="title">Signed in</div>
+<p>You are <b>{name}</b>, signed in as <b>{role}</b>.</p>"#,
+        name = escape(name),
+        role = escape(role),
+    );
+    if session {
+        card.push_str(SIGN_OUT);
+    }
+    page(frame, "Console", &card, None)
+}
+
+/// `s` with its first letter in capitals.
+fn capitalized(s: &str) -> String {
+    let mut c = s.chars();
+    c.next().map_or(String::new(), |first| {
+        first.to_uppercase().chain(c).collect()
+    })
+}
+
+/// `s` as a query parameter's value.
+pub(crate) fn encode(s: &str) -> String {
+    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
+}
````

`crates/data-avalanche-api/src/server.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/server.rs b/crates/data-avalanche-api/src/server.rs
index c5fa695..b2e0e55 100644
--- a/crates/data-avalanche-api/src/server.rs
+++ b/crates/data-avalanche-api/src/server.rs
@@ -13,7 +13,8 @@ use std::task::{Context, Poll};
 use std::time::Duration;
 
 use axum::Router;
-use axum::serve::Listener;
+use axum::extract::connect_info::Connected;
+use axum::serve::{IncomingStream, Listener};
 use data_avalanche_config::{HostPort, TlsIdentity};
 use rustls::ServerConfig;
 use rustls_pki_types::pem::PemObject;
@@ -114,13 +115,15 @@ impl Bound {
                     open,
                     idle: self.idle_timeout,
                 };
-                axum::serve(listener, router)
+                let service = router.into_make_service_with_connect_info::<Client>();
+                axum::serve(listener, service)
                     .with_graceful_shutdown(stop)
                     .await
             }
             Some(tls) => {
                 let listener = TlsListener::start(self.tcp, tls, open, self.idle_timeout)?;
-                axum::serve(listener, router)
+                let service = router.into_make_service_with_connect_info::<Client>();
+                axum::serve(listener, service)
                     .with_graceful_shutdown(stop)
                     .await
             }
@@ -227,6 +230,23 @@ impl<S: AsyncWrite + Unpin> AsyncWrite for Counted<S> {
     }
 }
 
+/// The address a request came from, as its connection says: a proxy's own, behind one. The
+/// console's sign-in counts failures by it.
+#[derive(Debug, Clone, Copy)]
+pub(crate) struct Client(pub SocketAddr);
+
+impl Connected<IncomingStream<'_, Capped>> for Client {
+    fn connect_info(stream: IncomingStream<'_, Capped>) -> Self {
+        Self(*stream.remote_addr())
+    }
+}
+
+impl Connected<IncomingStream<'_, TlsListener>> for Client {
+    fn connect_info(stream: IncomingStream<'_, TlsListener>) -> Self {
+        Self(*stream.remote_addr())
+    }
+}
+
 /// The plain listener, capped: it accepts only while a place is free.
 struct Capped {
     tcp: TcpListener,
````

`crates/data-avalanche-api/src/session.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/session.rs b/crates/data-avalanche-api/src/session.rs
index ef06425..c61c6b4 100644
--- a/crates/data-avalanche-api/src/session.rs
+++ b/crates/data-avalanche-api/src/session.rs
@@ -75,10 +75,6 @@ impl Sealer {
     }
 
     /// `plain`, sealed for `purpose`, as URL-safe base64.
-    #[cfg_attr(
-        not(test),
-        expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
-    )]
     pub(crate) fn seal(&self, purpose: &str, plain: &[u8]) -> String {
         let mut nonce = [0; NONCE_LEN];
         aws_lc_rs::rand::fill(&mut nonce).expect("the system's random source");
@@ -134,10 +130,6 @@ impl Sealer {
 }
 
 /// A random token, URL-safe: 32 bytes of the system's random source.
-#[cfg_attr(
-    not(test),
-    expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
-)]
 pub(crate) fn random_token() -> String {
     let mut b = [0; 32];
     aws_lc_rs::rand::fill(&mut b).expect("the system's random source");
@@ -198,10 +190,6 @@ impl std::fmt::Debug for By {
 
 /// A session too long for [`MAX_CHUNKS`] cookies.
 #[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
-#[cfg_attr(
-    not(test),
-    expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
-)]
 #[error(
     "the session needs {0} bytes of cookies, more than the {MAX_CHUNKS} cookies of {COOKIE_CHUNK} bytes a browser is sent: the provider's tokens are too long"
 )]
@@ -212,10 +200,6 @@ const SESSION: &str = "session";
 
 impl Session {
     /// A session signed in at `signed_in`, with a new anti-forgery token.
-    #[cfg_attr(
-        not(test),
-        expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
-    )]
     pub(crate) fn new(by: By, signed_in: i64) -> Self {
         Self {
             signed_in,
@@ -229,10 +213,6 @@ impl Session {
     ///
     /// # Errors
     /// [`TooLong`]: it needs more than [`MAX_CHUNKS`] cookies.
-    #[cfg_attr(
-        not(test),
-        expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
-    )]
     pub(crate) fn cookies(&self, sealer: &Sealer, max_age: u64) -> Result<Vec<String>, TooLong> {
         let plain = serde_json::to_vec(self).expect("a session serializes");
         let sealed = sealer.seal(SESSION, &plain);
@@ -253,6 +233,13 @@ impl Session {
         Ok(out)
     }
 
+    /// The `Set-Cookie` values that end a session: each of its cookies removed.
+    pub(crate) fn removed() -> Vec<String> {
+        (0..MAX_CHUNKS)
+            .map(|i| removed(&chunk_name(i), "/"))
+            .collect()
+    }
+
     /// The session the request's cookies hold: `None` without its cookie.
     ///
     /// # Errors
@@ -319,10 +306,6 @@ pub(crate) fn cookie<'h>(headers: &'h HeaderMap, name: &str) -> Option<&'h str>
 
 /// A `Set-Cookie` value: `HttpOnly` and `Secure`, for `path`, kept `max_age` seconds, `same_site`
 /// `Strict` or `Lax`.
-#[cfg_attr(
-    not(test),
-    expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
-)]
 pub(crate) fn set_cookie(
     name: &str,
     value: &str,
@@ -336,10 +319,6 @@ pub(crate) fn set_cookie(
 }
 
 /// The `Set-Cookie` value that removes cookie `name` at `path`.
-#[cfg_attr(
-    not(test),
-    expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
-)]
 pub(crate) fn removed(name: &str, path: &str) -> String {
     format!("{name}=; Path={path}; Max-Age=0; HttpOnly; Secure; SameSite=Strict")
 }
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-api --lib -- console:: auth::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/assets/console.css crates/data-avalanche-api/src/auth.rs crates/data-avalanche-api/src/console.rs crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/pages.rs crates/data-avalanche-api/src/server.rs crates/data-avalanche-api/src/session.rs
git commit -m "feat(api): the console's routes under /ui/: basic sign-in, sign-out, the session, the 401 and 403 pages"
```

---

### Task 4: The `oauth2` sign-in (`data-avalanche-api`)

**Files:**
- Create: `crates/data-avalanche-api/assets/sign-in.js`
- Modify: `crates/data-avalanche-api/src/auth.rs`
- Modify: `crates/data-avalanche-api/src/console.rs`
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Create: `crates/data-avalanche-api/src/oauth2.rs`
- Modify: `crates/data-avalanche-api/src/pages.rs`
- Modify: `crates/data-avalanche-api/src/session.rs`
- Modify: `crates/data-avalanche-api/src/test_support.rs`

**Interfaces:**
- Consumes: Tasks 1 and 3 (`SignIn`, `started`, `safe_next`, the pages, the cookie helpers);
  the token verifier (M11b); `globals.api.oauth2` (`ApiOauth2`: `issuer`, `discovery_url`,
  `client_id`, `client_secret`, `scopes`, `idp_name`).
- Produces (crate root): `SIGN_IN_WITHIN` (600 s), `REFRESH_AHEAD` (60 s), `AUTO_REDIRECT` (5 s),
  `SIGN_IN_COOKIE` (`"da_oauth2"`), `ID_COOKIE` (`"da_id"`).
- Produces (crate-private): `oauth2::Provider` (`Auth::provider`): `endpoints(&ApiOauth2) ->
  Result<Endpoints, String>`, `cached`, `exchange`, `refresh`; `Endpoints { authorization, token,
  end_session, secret_in_form }`, `Tokens { access, refresh, id }`; `challenge(verifier) ->
  String` (PKCE `S256`); `origin(url)`; `expires_in(token, now)`; the routes `start`, `callback`,
  `refresh`, and `renewed`, `due`, `end_session`, used by Task 3's routes; `Auth::verify_token`;
  `same_secret`; `session::as_sent`.
- Tests (`test_support.rs`): the issuer serves a discovery document, an authorization endpoint
  that answers with a code, a token endpoint (`SignIns`, each `TokenRequest` recorded) and an
  end-session endpoint.

- [ ] **Step 1: Write the failing tests**

The tests (oauth2): `the_sign_in_page_names_the_provider_and_goes_there_unless_told_not_to`,
`a_sign_in_goes_to_the_provider_with_pkce_and_comes_back_signed_in`,
`a_callback_that_isnt_the_sign_in_s_own_fails_without_a_loop`,
`a_token_whose_roles_map_to_nothing_is_forbidden_at_sign_in`,
`a_confidential_client_authenticates_with_its_secret`,
`a_session_whose_token_is_about_to_expire_is_refreshed_once`,
`a_refresh_the_provider_refuses_ends_the_session_saying_why`,
`the_app_refreshes_its_session_with_its_anti_forgery_token`,
`the_sign_out_goes_on_to_the_provider`, `a_pkce_challenge_is_the_verifier_s_sha_256` (the FIPS
180-2 `abc` vector). The new module holds only its tests for now.

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index b145c63..0c07c11 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -22,6 +22,7 @@ mod history;
 mod limit;
 mod live;
 mod nodes;
+mod oauth2;
 mod openapi;
 mod pages;
 mod peers;
````

`crates/data-avalanche-api/src/oauth2.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/oauth2.rs b/crates/data-avalanche-api/src/oauth2.rs
new file mode 100644
index 0000000..52135c3
--- /dev/null
+++ b/crates/data-avalanche-api/src/oauth2.rs
@@ -0,0 +1,438 @@
+#[cfg(test)]
+mod tests {
+    use std::net::SocketAddr;
+    use std::sync::Arc;
+    use std::sync::atomic::Ordering;
+
+    use data_avalanche_config::{Console, FingerprintKey, Secret};
+
+    use super::*;
+    use crate::Auth;
+    use crate::test_support::{Issuer, api, request, serve, sessions};
+
+    const HOUR: Duration = Duration::from_secs(3600);
+    const PUBLIC: &str = "https://console.example";
+
+    /// The console of the tests' oauth2 nodes: sessions of `ttl`, reached at [`PUBLIC`].
+    fn console(ttl: Duration) -> Console {
+        Console {
+            public_url: Some(PUBLIC.into()),
+            ..sessions(ttl)
+        }
+    }
+
+    /// A node signing in through `idp`, as `change` sets its `oauth2`.
+    async fn node(idp: &Issuer, change: impl FnOnce(&mut ApiOauth2)) -> SocketAddr {
+        let mut a = idp.oauth2_api("roles");
+        change(a.oauth2.as_mut().unwrap());
+        serve(api(Arc::new(Auth::new(&a, &console(8 * HOUR))))).await
+    }
+
+    fn header_of<'h>(head: &'h str, name: &str) -> Vec<&'h str> {
+        head.lines()
+            .filter_map(|l| l.split_once(": "))
+            .filter(|(n, _)| n.eq_ignore_ascii_case(name))
+            .map(|(_, v)| v)
+            .collect()
+    }
+
+    /// The `Cookie` a browser sends `path` after these `Set-Cookie`s: the ones it keeps whose
+    /// path covers it.
+    fn kept_for(head: &str, path: &str) -> String {
+        header_of(head, "set-cookie")
+            .into_iter()
+            .filter(|c| !c.contains("Max-Age=0"))
+            .filter(|c| {
+                c.split("; ")
+                    .find_map(|a| a.strip_prefix("Path="))
+                    .is_some_and(|p| path.starts_with(p))
+            })
+            .map(|c| c.split(';').next().unwrap())
+            .collect::<Vec<_>>()
+            .join("; ")
+    }
+
+    /// The query of `url`, by name.
+    fn query(url: &str) -> BTreeMap<String, String> {
+        url::Url::parse(url)
+            .unwrap()
+            .query_pairs()
+            .into_owned()
+            .collect()
+    }
+
+    /// A sign-in through the provider, as a browser makes it: the start, the provider's
+    /// approval, the callback. The callback's code, head and page.
+    async fn signed_in(addr: SocketAddr, next: &str) -> (u16, String, String) {
+        let start = format!("/ui/oauth2/start?next={}", pages::encode(next));
+        let (code, head, _) = request(addr, "GET", &start, &[], "").await;
+        assert_eq!(code, 303);
+        let at_provider = header_of(&head, "location")[0].to_owned();
+        let left_with = kept_for(&head, CALLBACK);
+        let back = reqwest::Client::builder()
+            .redirect(reqwest::redirect::Policy::none())
+            .build()
+            .unwrap()
+            .get(&at_provider)
+            .send()
+            .await
+            .unwrap();
+        let back = back.headers()["location"].to_str().unwrap().to_owned();
+        let path = back
+            .strip_prefix(PUBLIC)
+            .expect("back at the console")
+            .to_owned();
+        request(addr, "GET", &path, &[("Cookie", left_with.as_str())], "").await
+    }
+
+    /// Admin_Console §9: the sign-in page names the provider (`idp_name`, else the issuer's
+    /// host) and goes there after 5 s, but not after a sign-out, a failed sign-in, or a choice to
+    /// stay; the provider's origin may receive the sign-out form.
+    #[tokio::test]
+    async fn the_sign_in_page_names_the_provider_and_goes_there_unless_told_not_to() {
+        let idp = Issuer::start().await;
+        let addr = node(&idp, |o| o.idp_name = Some("Company SSO".into())).await;
+        let (code, head, page) = request(addr, "GET", "/ui/sign-in?next=/ui/x", &[], "").await;
+        assert_eq!(code, 200);
+        assert!(
+            page.contains(r#"<a class="btn" id="continue" href="/ui/oauth2/start?next=%2Fui%2Fx">Continue with Company SSO</a>"#),
+            "{page}"
+        );
+        assert!(page.contains(r#"data-seconds="5""#), "{page}");
+        assert!(
+            page.contains(r#"<script src="/ui/assets/sign-in.js"></script>"#),
+            "{page}"
+        );
+        let csp = header_of(&head, "content-security-policy")[0].to_owned();
+        assert!(
+            csp.contains(&format!("form-action 'self' {}", idp.url)),
+            "{csp}"
+        );
+        for no_loop in ["signed_out", "failed", "stay"] {
+            let (_, _, page) =
+                request(addr, "GET", &format!("/ui/sign-in?{no_loop}"), &[], "").await;
+            assert!(!page.contains("data-seconds"), "{no_loop}: {page}");
+        }
+        let addr = node(&idp, |_| {}).await;
+        let host = url::Url::parse(&idp.url)
+            .unwrap()
+            .host_str()
+            .unwrap()
+            .to_owned();
+        let (_, _, page) = request(addr, "GET", "/ui/sign-in", &[], "").await;
+        assert!(
+            page.contains(&format!("Continue with {host}</a>")),
+            "{page}"
+        );
+        let (code, _, script) = request(addr, "GET", "/ui/assets/sign-in.js", &[], "").await;
+        assert!(code == 200 && script.contains("countdown"), "{script}");
+    }
+
+    /// Admin_Console §9, config §6.3.1: authorization code with PKCE (S256), the code exchanged
+    /// by the server as the public client, and a session that every page then reads.
+    #[tokio::test]
+    async fn a_sign_in_goes_to_the_provider_with_pkce_and_comes_back_signed_in() {
+        let idp = Issuer::start().await;
+        let addr = node(&idp, |_| {}).await;
+        let (code, head, page) = signed_in(addr, "/ui/pipelines/orders").await;
+        assert_eq!(code, 200, "{page}");
+        assert!(
+            page.contains(r#"<meta http-equiv="refresh" content="0;url=/ui/pipelines/orders">"#),
+            "the page goes on from the console's own: {page}"
+        );
+        let asked = idp.sign_ins.asked.lock().unwrap()[0].clone();
+        assert_eq!(asked["response_type"], "code");
+        assert_eq!(asked["client_id"], "cdc-console");
+        assert_eq!(
+            asked["redirect_uri"],
+            format!("{PUBLIC}/ui/oauth2/callback")
+        );
+        assert_eq!(asked["scope"], "openid profile");
+        assert_eq!(asked["code_challenge_method"], "S256");
+        assert_eq!(asked["state"].len(), 43);
+        let (form, authorization) = idp.sign_ins.token_requests.lock().unwrap()[0].clone();
+        assert_eq!(form["grant_type"], "authorization_code");
+        assert_eq!(
+            form["client_id"], "cdc-console",
+            "a public client names itself"
+        );
+        assert_eq!(authorization, None);
+        let set = header_of(&head, "set-cookie");
+        assert!(
+            set.iter()
+                .any(|c| c.starts_with("da_oauth2=;") && c.contains("Max-Age=0")),
+            "the sign-in's state is spent: {set:?}"
+        );
+        assert!(
+            set.iter()
+                .any(|c| c.starts_with("da_id=") && c.contains("Path=/ui/sign-out")),
+            "{set:?}"
+        );
+        let cookie = kept_for(&head, "/ui/session");
+        assert!(
+            !cookie.contains("da_id"),
+            "the ID token goes only to the sign-out"
+        );
+        let (code, _, text) = request(
+            addr,
+            "GET",
+            "/ui/session",
+            &[("Cookie", cookie.as_str())],
+            "",
+        )
+        .await;
+        assert_eq!(code, 200, "{text}");
+        let v: Value = serde_json::from_str(&text).unwrap();
+        assert_eq!(
+            (&v["name"], &v["role"], &v["auth"]),
+            (&json!("alice.smith"), &json!("operator"), &json!("oauth2"))
+        );
+    }
+
+    /// A callback that isn't the sign-in's own (no state cookie, another state, the provider's
+    /// refusal) fails, and its page doesn't go back to the provider by itself.
+    #[tokio::test]
+    async fn a_callback_that_isnt_the_sign_in_s_own_fails_without_a_loop() {
+        let idp = Issuer::start().await;
+        let addr = node(&idp, |_| {}).await;
+        let (code, _, page) =
+            request(addr, "GET", "/ui/oauth2/callback?code=c&state=s", &[], "").await;
+        assert_eq!(code, 400);
+        assert!(page.contains("began in another browser"), "{page}");
+        assert!(!page.contains("data-seconds"), "no countdown: {page}");
+        let (_, head, _) = request(addr, "GET", "/ui/oauth2/start?next=/ui/x", &[], "").await;
+        let left_with = kept_for(&head, CALLBACK);
+        let with = [("Cookie", left_with.as_str())];
+        let (code, _, page) = request(
+            addr,
+            "GET",
+            "/ui/oauth2/callback?code=c&state=another",
+            &with,
+            "",
+        )
+        .await;
+        assert_eq!(code, 400);
+        assert!(page.contains("isn&#39;t this sign-in&#39;s"), "{page}");
+        let refused = "/ui/oauth2/callback?error=access_denied&error_description=The+user+said+no";
+        let (code, head, page) = request(addr, "GET", refused, &with, "").await;
+        assert_eq!(code, 400);
+        assert!(
+            page.contains("refused the sign-in: The user said no"),
+            "{page}"
+        );
+        assert!(!page.contains("data-seconds"), "{page}");
+        assert!(
+            header_of(&head, "set-cookie")
+                .iter()
+                .any(|c| c.starts_with("da_oauth2=;"))
+        );
+        assert!(
+            idp.sign_ins.token_requests.lock().unwrap().is_empty(),
+            "nothing exchanged"
+        );
+    }
+
+    /// Mockup `02-sign-in.html`: a token whose roles map to nothing is "Forbidden" at sign-in,
+    /// naming the roles it carried; no session starts.
+    #[tokio::test]
+    async fn a_token_whose_roles_map_to_nothing_is_forbidden_at_sign_in() {
+        let idp = Issuer::start().await;
+        *idp.sign_ins.roles.lock().unwrap() = json!(["cdc-ops", "reporting"]);
+        let addr = node(&idp, |_| {}).await;
+        let (code, head, page) = signed_in(addr, "/ui/").await;
+        assert_eq!(code, 403);
+        assert!(
+            page.contains("You are <b>alice.smith</b>, and the token&#39;s roles (cdc-ops, reporting) name no role this node maps."),
+            "{page}"
+        );
+        assert!(
+            !header_of(&head, "set-cookie")
+                .iter()
+                .any(|c| c.starts_with("da_session="))
+        );
+    }
+
+    /// A confidential client authenticates at the token endpoint with HTTP Basic.
+    #[tokio::test]
+    async fn a_confidential_client_authenticates_with_its_secret() {
+        let idp = Issuer::start().await;
+        let secret = Secret::new("s3cr:t", &FingerprintKey::from_bytes([1; 32]));
+        let addr = node(&idp, |o| o.client_secret = Some(secret)).await;
+        assert_eq!(signed_in(addr, "/ui/").await.0, 200);
+        let (form, authorization) = idp.sign_ins.token_requests.lock().unwrap()[0].clone();
+        assert!(!form.contains_key("client_secret") && !form.contains_key("client_id"));
+        assert_eq!(
+            authorization.as_deref(),
+            Some(format!("Basic {}", STANDARD.encode("cdc-console:s3cr%3At")).as_str())
+        );
+    }
+
+    /// A session about to expire is refreshed by the page asked for, once however many pages ask
+    /// at once; it keeps when it signed in, so it ends when it would have.
+    #[tokio::test]
+    async fn a_session_whose_token_is_about_to_expire_is_refreshed_once() {
+        let idp = Issuer::start().await;
+        let addr = node(&idp, |_| {}).await;
+        let mut claims = idp.claims(json!(["operator"]));
+        claims["exp"] = json!(Utc::now().timestamp() + 30);
+        let old = Session::new(
+            By::Oauth2 {
+                access: idp.sign("k1", &claims),
+                refresh: Some("refresh-0".into()),
+            },
+            Utc::now().timestamp() - 600,
+        );
+        let cookie = crate::test_support::session_cookie(&old);
+        let page = |cookie: String| async move {
+            request(addr, "GET", "/ui/", &[("Cookie", cookie.as_str())], "").await
+        };
+        let (a, b) = tokio::join!(page(cookie.clone()), page(cookie));
+        for (code, head, body) in [&a, &b] {
+            assert_eq!(*code, 200, "{body}");
+            let renewed = kept_for(head, "/ui/");
+            let sealer = crate::session::Sealer::new(crate::test_support::SESSION_KEY);
+            let mut h = HeaderMap::new();
+            h.insert(header::COOKIE, renewed.parse().unwrap());
+            let new = Session::of(&h, &sealer).unwrap().unwrap();
+            assert_eq!((new.signed_in, &new.csrf), (old.signed_in, &old.csrf));
+            assert_ne!(new.by, old.by, "a new token");
+        }
+        assert_eq!(idp.sign_ins.refreshes.load(Ordering::SeqCst), 1);
+    }
+
+    /// A token expired whose refresh the provider refuses ends the session, saying so.
+    #[tokio::test]
+    async fn a_refresh_the_provider_refuses_ends_the_session_saying_why() {
+        let idp = Issuer::start().await;
+        idp.sign_ins.refusing.store(true, Ordering::SeqCst);
+        let addr = node(&idp, |_| {}).await;
+        let mut claims = idp.claims(json!(["operator"]));
+        claims["exp"] = json!(Utc::now().timestamp() - 120);
+        let old = Session::new(
+            By::Oauth2 {
+                access: idp.sign("k1", &claims),
+                refresh: Some("refresh-0".into()),
+            },
+            Utc::now().timestamp() - 600,
+        );
+        let cookie = crate::test_support::session_cookie(&old);
+        let (code, _, page) =
+            request(addr, "GET", "/ui/x", &[("Cookie", cookie.as_str())], "").await;
+        assert_eq!(code, 401);
+        assert!(
+            page.contains("Your session has ended")
+                && page.contains("The provider&#39;s token expired and couldn&#39;t be refreshed: the provider answered 400: the refresh token was revoked."),
+            "{page}"
+        );
+    }
+
+    /// The console's app refreshes its session with the session's anti-forgery token, from its
+    /// own page; a session without a refresh token can't be refreshed.
+    #[tokio::test]
+    async fn the_app_refreshes_its_session_with_its_anti_forgery_token() {
+        let idp = Issuer::start().await;
+        let addr = node(&idp, |_| {}).await;
+        let (_, head, _) = signed_in(addr, "/ui/").await;
+        let cookie = kept_for(&head, "/ui/session");
+        let (_, _, text) = request(
+            addr,
+            "GET",
+            "/ui/session",
+            &[("Cookie", cookie.as_str())],
+            "",
+        )
+        .await;
+        let csrf = serde_json::from_str::<Value>(&text).unwrap()["csrf"]
+            .as_str()
+            .unwrap()
+            .to_owned();
+        let (code, _, _) = request(
+            addr,
+            "POST",
+            "/ui/session/refresh",
+            &[("Cookie", cookie.as_str())],
+            "",
+        )
+        .await;
+        assert_eq!(code, 403, "without the token");
+        let with = [
+            ("Cookie", cookie.as_str()),
+            (crate::CSRF_HEADER, csrf.as_str()),
+        ];
+        let (code, head, text) = request(addr, "POST", "/ui/session/refresh", &with, "").await;
+        assert_eq!(code, 200, "{text}");
+        assert_eq!(serde_json::from_str::<Value>(&text).unwrap()["csrf"], csrf);
+        assert!(kept_for(&head, "/ui/").starts_with("da_session="));
+        assert_eq!(idp.sign_ins.refreshes.load(Ordering::SeqCst), 1);
+        let none = Session::new(
+            By::Oauth2 {
+                access: idp.token(json!(["operator"])),
+                refresh: None,
+            },
+            Utc::now().timestamp(),
+        );
+        let cookie = crate::test_support::session_cookie(&none);
+        let with = [
+            ("Cookie", cookie.as_str()),
+            (crate::CSRF_HEADER, none.csrf.as_str()),
+        ];
+        assert_eq!(
+            request(addr, "POST", "/ui/session/refresh", &with, "")
+                .await
+                .0,
+            409
+        );
+    }
+
+    /// Admin_Console §9: the sign-out clears the session and signs out at the provider, with the
+    /// ID token kept and the way back to the sign-in.
+    #[tokio::test]
+    async fn the_sign_out_goes_on_to_the_provider() {
+        let idp = Issuer::start().await;
+        let addr = node(&idp, |_| {}).await;
+        let (_, head, _) = signed_in(addr, "/ui/").await;
+        let cookie = kept_for(&head, SIGN_OUT);
+        assert!(cookie.contains("da_id="), "{cookie}");
+        let (code, head, _) =
+            request(addr, "POST", SIGN_OUT, &[("Cookie", cookie.as_str())], "").await;
+        assert_eq!(code, 303);
+        let to = header_of(&head, "location")[0];
+        assert!(to.starts_with(&format!("{}/end-session?", idp.url)), "{to}");
+        let q = query(to);
+        assert_eq!(q["client_id"], "cdc-console");
+        assert_eq!(
+            q["post_logout_redirect_uri"],
+            format!("{PUBLIC}/ui/sign-in?signed_out")
+        );
+        assert_eq!(q["id_token_hint"].split('.').count(), 3, "a JWT");
+        let removed: Vec<&str> = header_of(&head, "set-cookie")
+            .into_iter()
+            .filter(|c| c.contains("Max-Age=0"))
+            .map(|c| c.split('=').next().unwrap())
+            .collect();
+        assert_eq!(
+            removed,
+            [
+                "da_session",
+                "da_session.1",
+                "da_session.2",
+                "da_session.3",
+                "da_id"
+            ]
+        );
+    }
+
+    /// RFC 7636 §4.2: `S256` is the URL-safe base64 of the verifier's SHA-256, unpadded; the
+    /// digest of `abc` is FIPS 180-2's first example.
+    #[test]
+    fn a_pkce_challenge_is_the_verifier_s_sha_256() {
+        let fips = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
+        assert_eq!(
+            challenge("abc"),
+            "ungWv48Bz-pBQUDeXa4iI7ADYaOWF3qctBD_YfIAFa0"
+        );
+        let digest = URL_SAFE_NO_PAD.decode(challenge("abc")).unwrap();
+        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
+        assert_eq!(hex, fips);
+    }
+}
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index 6e203f0..618ded7 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -116,14 +116,7 @@ pub(crate) fn basic_api(password: &str) -> data_avalanche_config::Api {
 /// The `Cookie` header that carries `session`, sealed with [`SESSION_KEY`].
 pub(crate) fn session_cookie(session: &crate::session::Session) -> String {
     let sealer = crate::session::Sealer::new(SESSION_KEY);
-    session
-        .cookies(&sealer, 3600)
-        .unwrap()
-        .iter()
-        .filter(|c| !c.contains("Max-Age=0"))
-        .map(|c| c.split(';').next().unwrap().to_owned())
-        .collect::<Vec<_>>()
-        .join("; ")
+    crate::session::as_sent(&session.cookies(&sealer, 3600).unwrap())
 }
 
 /// A basic session of user `ops`, who gave `password`, signed in `ago` before now.
@@ -405,7 +398,9 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
 }
 
 /// An identity provider: ES256 keys made at run time, its JWKS at `/jwks` and its OIDC discovery
-/// document at `/.well-known/openid-configuration`, on a loopback port.
+/// document at `/.well-known/openid-configuration`, on a loopback port. It signs the console's
+/// client in too: `/authorize` approves at once, `/token` exchanges a code for its PKCE verifier
+/// and refreshes, `/end-session` signs out.
 pub(crate) struct Issuer {
     pub url: String,
     keys: Arc<Mutex<Vec<(String, KeyPair)>>>,
@@ -413,6 +408,27 @@ pub(crate) struct Issuer {
     pub fetches: Arc<AtomicUsize>,
     down: Arc<AtomicBool>,
     hanging: Arc<AtomicBool>,
+    pub sign_ins: Arc<SignIns>,
+}
+
+/// A token request's form, and its `Authorization`.
+pub(crate) type TokenRequest = (BTreeMap<String, String>, Option<String>);
+
+/// What the provider did for the console's client.
+#[derive(Default)]
+pub(crate) struct SignIns {
+    /// The roles of the tokens it issues.
+    pub roles: Mutex<Value>,
+    /// The authorization requests' queries, as asked.
+    pub asked: Mutex<Vec<BTreeMap<String, String>>>,
+    /// Each code issued, with its PKCE challenge and redirect URI.
+    codes: Mutex<BTreeMap<String, (String, String)>>,
+    /// The token requests' forms, and their `Authorization`.
+    pub token_requests: Mutex<Vec<TokenRequest>>,
+    /// Refreshes asked for.
+    pub refreshes: AtomicUsize,
+    /// Refuses every refresh, as a provider that revoked the sign-in.
+    pub refusing: AtomicBool,
 }
 
 #[derive(Clone)]
@@ -422,6 +438,7 @@ struct Served {
     fetches: Arc<AtomicUsize>,
     down: Arc<AtomicBool>,
     hanging: Arc<AtomicBool>,
+    sign_ins: Arc<SignIns>,
 }
 
 fn jwk(kid: &str, key: &KeyPair) -> Value {
@@ -450,7 +467,115 @@ async fn jwks(State(s): State<Served>) -> Result<Json<Value>, axum::http::Status
 }
 
 async fn discovery(State(s): State<Served>) -> Json<Value> {
-    Json(json!({ "issuer": s.url, "jwks_uri": format!("{}/jwks", s.url) }))
+    Json(json!({
+        "issuer": s.url,
+        "jwks_uri": format!("{}/jwks", s.url),
+        "authorization_endpoint": format!("{}/authorize", s.url),
+        "token_endpoint": format!("{}/token", s.url),
+        "end_session_endpoint": format!("{}/end-session", s.url),
+    }))
+}
+
+/// Approves at once: back to the client's redirect URI with a code and the state.
+async fn authorize(
+    State(s): State<Served>,
+    axum::extract::RawQuery(raw): axum::extract::RawQuery,
+) -> axum::response::Response {
+    use axum::response::IntoResponse;
+    let q: BTreeMap<String, String> =
+        url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
+            .into_owned()
+            .collect();
+    s.sign_ins.asked.lock().unwrap().push(q.clone());
+    let code = format!("code-{}", s.sign_ins.asked.lock().unwrap().len());
+    let redirect = q.get("redirect_uri").cloned().unwrap_or_default();
+    s.sign_ins.codes.lock().unwrap().insert(
+        code.clone(),
+        (
+            q.get("code_challenge").cloned().unwrap_or_default(),
+            redirect.clone(),
+        ),
+    );
+    let state =
+        url::form_urlencoded::byte_serialize(q.get("state").map_or("", String::as_str).as_bytes())
+            .collect::<String>();
+    (
+        StatusCode::SEE_OTHER,
+        [(
+            axum::http::header::LOCATION,
+            format!("{redirect}?code={code}&state={state}"),
+        )],
+    )
+        .into_response()
+}
+
+/// Exchanges a code whose PKCE verifier matches, or refreshes, for tokens with the provider's
+/// roles.
+async fn token(
+    State(s): State<Served>,
+    headers: HeaderMap,
+    form: Bytes,
+) -> axum::response::Response {
+    use axum::response::IntoResponse;
+    let form: BTreeMap<String, String> = url::form_urlencoded::parse(&form).into_owned().collect();
+    let authorization = headers
+        .get("authorization")
+        .map(|v| v.to_str().unwrap().to_owned());
+    s.sign_ins
+        .token_requests
+        .lock()
+        .unwrap()
+        .push((form.clone(), authorization));
+    let refused = |why: &str| {
+        (
+            StatusCode::BAD_REQUEST,
+            Json(json!({ "error": "invalid_grant", "error_description": why })),
+        )
+            .into_response()
+    };
+    let n = match form.get("grant_type").map(String::as_str) {
+        Some("authorization_code") => {
+            let code = form.get("code").cloned().unwrap_or_default();
+            let Some((challenge, redirect)) = s.sign_ins.codes.lock().unwrap().remove(&code) else {
+                return refused("no such code");
+            };
+            let verifier = form.get("code_verifier").cloned().unwrap_or_default();
+            if crate::oauth2::challenge(&verifier) != challenge
+                || form.get("redirect_uri") != Some(&redirect)
+            {
+                return refused("the verifier or the redirect URI doesn't match");
+            }
+            0
+        }
+        Some("refresh_token") => {
+            if s.sign_ins.refusing.load(Ordering::SeqCst) {
+                return refused("the refresh token was revoked");
+            }
+            s.sign_ins.refreshes.fetch_add(1, Ordering::SeqCst) + 1
+        }
+        _ => return refused("unsupported grant"),
+    };
+    let now = jsonwebtoken::get_current_timestamp();
+    let claims = json!({
+        "iss": s.url, "aud": "cdc", "sub": "alice", "preferred_username": "alice.smith",
+        "exp": now + 600, "roles": s.sign_ins.roles.lock().unwrap().clone(),
+    });
+    let keys = s.keys.lock().unwrap();
+    let (kid, key) = &keys[0];
+    let access = sign_with(key, Some(kid), &claims);
+    let id = sign_with(
+        key,
+        Some(kid),
+        &json!({ "iss": s.url, "aud": "cdc-console", "sub": "alice", "exp": now + 600 }),
+    );
+    Json(json!({
+        "access_token": access,
+        "token_type": "Bearer",
+        "expires_in": 600,
+        "refresh_token": format!("refresh-{n}"),
+        "id_token": id,
+    }))
+    .into_response()
 }
 
 impl Issuer {
@@ -462,16 +587,24 @@ impl Issuer {
         let fetches = Arc::new(AtomicUsize::new(0));
         let down = Arc::new(AtomicBool::new(false));
         let hanging = Arc::new(AtomicBool::new(false));
+        let sign_ins = Arc::new(SignIns {
+            roles: Mutex::new(json!(["operator"])),
+            ..SignIns::default()
+        });
         let served = Served {
             down: Arc::clone(&down),
             hanging: Arc::clone(&hanging),
             url: url.clone(),
             keys: Arc::clone(&keys),
             fetches: Arc::clone(&fetches),
+            sign_ins: Arc::clone(&sign_ins),
         };
         let app = axum::Router::new()
             .route("/jwks", get_route(jwks))
             .route("/.well-known/openid-configuration", get_route(discovery))
+            .route("/authorize", get_route(authorize))
+            .route("/token", axum::routing::post(token))
+            .route("/end-session", get_route(|| async { "signed out" }))
             .with_state(served);
         tokio::spawn(async move { axum::serve(listener, app).await });
         let issuer = Self {
@@ -480,6 +613,7 @@ impl Issuer {
             fetches,
             down,
             hanging,
+            sign_ins,
         };
         issuer.add_key("k1");
         issuer
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api --lib -- oauth2:: console:: auth::`
Expected: the API's tests don't compile: the new module's find no `challenge`, `CALLBACK`, `SIGN_OUT`, `pages` or `session::as_sent` (nor the imports the code above them brings: `json!`, `Value`, `Session`, `By`, `Utc`, `ApiOauth2`, `BTreeMap`, base64's engines).

- [ ] **Step 3: Implement**

`Provider` reads with the API's `reqwest` client (no redirect followed, a body over 1 MiB
refused). The refresh holds a `tokio::sync::Mutex` and keeps each refresh token's outcome, by
its SHA-256, for 30 s. `assets/sign-in.js` is the page's only script: the countdown to
`AUTO_REDIRECT`.

`crates/data-avalanche-api/assets/sign-in.js`:

````diff
diff --git a/crates/data-avalanche-api/assets/sign-in.js b/crates/data-avalanche-api/assets/sign-in.js
new file mode 100644
index 0000000..6dd65a2
--- /dev/null
+++ b/crates/data-avalanche-api/assets/sign-in.js
@@ -0,0 +1,28 @@
+// The oauth2 sign-in page goes to the provider by itself after a few seconds, unless the person
+// stays (Admin_Console §9). Without this script, the button is all there is.
+(function () {
+  var countdown = document.getElementById("countdown");
+  var go = document.getElementById("continue");
+  if (!countdown || !go) {
+    return;
+  }
+  var total = Number(countdown.getAttribute("data-seconds"));
+  var left = total;
+  var shown = document.getElementById("left");
+  var bar = document.getElementById("bar");
+  var timer = window.setInterval(function () {
+    left -= 1;
+    if (left <= 0) {
+      window.clearInterval(timer);
+      window.location.assign(go.getAttribute("href"));
+      return;
+    }
+    shown.textContent = String(left);
+    bar.style.width = String((100 * left) / total) + "%";
+  }, 1000);
+  document.getElementById("stay").addEventListener("click", function (e) {
+    e.preventDefault();
+    window.clearInterval(timer);
+    countdown.hidden = true;
+  });
+})();
````

`crates/data-avalanche-api/src/auth.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/auth.rs b/crates/data-avalanche-api/src/auth.rs
index 863a5b9..d4b0798 100644
--- a/crates/data-avalanche-api/src/auth.rs
+++ b/crates/data-avalanche-api/src/auth.rs
@@ -22,7 +22,9 @@ use axum::http::{HeaderMap, HeaderValue};
 use base64::Engine;
 use base64::engine::general_purpose::STANDARD;
 use chrono::{DateTime, SecondsFormat, Utc};
-use data_avalanche_config::{Api as ApiConfig, ApiAuth, Console, Credentials, Role, Secret};
+use data_avalanche_config::{
+    Api as ApiConfig, ApiAuth, ApiOauth2, Console, Credentials, Role, Secret,
+};
 use jsonwebtoken::jwk::JwkSet;
 use jsonwebtoken::{AlgorithmFamily, DecodingKey, Validation, decode, decode_header};
 use serde_json::Value;
@@ -30,6 +32,7 @@ use subtle::ConstantTimeEq;
 use tokio::sync::watch;
 
 use crate::console::Attempts;
+use crate::oauth2::Provider;
 use crate::session::{By, Sealer, Session, Unsealed, session_cookies};
 
 /// A JWKS fetch, or a discovery, takes at most this long.
@@ -157,6 +160,8 @@ struct Settings {
     sessions: Option<(Secret, Duration)>,
     /// `globals.console.enabled`.
     console: bool,
+    /// `globals.console.public_url`.
+    public_url: Option<String>,
 }
 
 impl Settings {
@@ -174,6 +179,7 @@ impl Settings {
             username_claim: console.username_claim.clone(),
             sessions,
             console: console.enabled,
+            public_url: console.public_url.clone(),
         }
     }
 }
@@ -225,6 +231,8 @@ pub(crate) struct SignIn {
     pub auth: ApiAuth,
     pub sessions: Option<Arc<Sessions>>,
     pub credentials: Option<Credentials>,
+    pub oauth2: Option<ApiOauth2>,
+    pub public_url: Option<String>,
 }
 
 /// The node's authentication, swapped by a reload.
@@ -234,6 +242,8 @@ pub struct Auth {
     timing: Timing,
     /// The failed basic sign-ins of each address (Admin_Console §9).
     pub(crate) attempts: Attempts,
+    /// The `oauth2` provider's client, for the console's sign-in.
+    pub(crate) provider: Provider,
 }
 
 impl Auth {
@@ -277,6 +287,7 @@ impl Auth {
             }),
             timing,
             attempts: Attempts::default(),
+            provider: Provider::default(),
         }
     }
 
@@ -306,9 +317,27 @@ impl Auth {
             auth: s.auth,
             sessions: current.sessions.clone(),
             credentials: s.credentials.clone(),
+            oauth2: s.oauth2.clone(),
+            public_url: s.public_url.clone(),
         })
     }
 
+    /// The caller `token` names, verified as a bearer token is (`jwt`, `oauth2`): what the
+    /// console's sign-in checks of the provider's token before it starts a session.
+    pub(crate) async fn verify_token(&self, token: &str) -> Result<Principal, Denied> {
+        let mode = Arc::clone(
+            &self
+                .current
+                .read()
+                .unwrap_or_else(PoisonError::into_inner)
+                .mode,
+        );
+        match &*mode {
+            Mode::Token(verifier) => verifier.verify_token(token).await,
+            _ => Err(Denied::Unavailable(MISCONFIGURED.into())),
+        }
+    }
+
     /// The caller of a request with these headers.
     pub(crate) async fn verify(&self, headers: &HeaderMap) -> Result<Principal, Denied> {
         self.verify_caller(headers).await.map(|v| v.principal)
@@ -500,6 +529,11 @@ fn same(given: &str, expected: &str) -> subtle::Choice {
         .ct_eq(blake3::hash(expected.as_bytes()).as_bytes())
 }
 
+/// Whether `given` is `expected`, compared in constant time.
+pub(crate) fn same_secret(given: &str, expected: &str) -> bool {
+    bool::from(same(given, expected))
+}
+
 /// Whether `user` and `password` are the configured ones, both compared in full whichever
 /// differs.
 pub(crate) fn basic_matches(c: &Credentials, user: &str, password: &str) -> bool {
````

`crates/data-avalanche-api/src/console.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/console.rs b/crates/data-avalanche-api/src/console.rs
index 1a0833e..9e874ef 100644
--- a/crates/data-avalanche-api/src/console.rs
+++ b/crates/data-avalanche-api/src/console.rs
@@ -21,12 +21,12 @@ use axum::routing::{get, post};
 use axum::{Extension, Router};
 use chrono::{SecondsFormat, Utc};
 use data_avalanche_config::ApiAuth;
-use serde_json::json;
+use serde_json::{Value, json};
 
-use crate::auth::{Denied, SignIn, Verified};
+use crate::auth::{Denied, Sessions, SignIn, Verified};
 use crate::pages::{self, Frame};
-use crate::session::{By, KEPT_AFTER, Session, sealed_value};
-use crate::{Api, body};
+use crate::session::{By, KEPT_AFTER, Session, TooLong, as_sent, sealed_value};
+use crate::{Api, body, oauth2};
 
 /// Failed basic sign-ins in a row from one address before it waits.
 pub const SIGN_IN_ATTEMPTS: u32 = 5;
@@ -53,23 +53,26 @@ pub(crate) fn routes(api: &Api) -> Router<Api> {
         .route("/ui/sign-in", get(sign_in_page).post(sign_in))
         .route("/ui/sign-out", post(sign_out))
         .route("/ui/session", get(session))
+        .route("/ui/session/refresh", post(oauth2::refresh))
+        .route("/ui/oauth2/start", get(oauth2::start))
+        .route("/ui/oauth2/callback", get(oauth2::callback))
         .route("/ui/assets/{file}", get(asset))
         .layer(from_fn_with_state(api.clone(), guard))
 }
 
 /// Serves `/ui/` while the console is enabled, and sets every answer's security headers.
 async fn guard(State(api): State<Api>, req: Request, next: Next) -> Response {
-    if api.auth.sign_in().is_none() {
+    let Some(sign_in) = api.auth.sign_in() else {
         return (
             StatusCode::NOT_FOUND,
             [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
             "The console is off on this cluster: globals.console.enabled.\n",
         )
             .into_response();
-    }
+    };
     let mut r = next.run(req).await;
     let h = r.headers_mut();
-    let csp = policy("");
+    let csp = policy(&oauth2::form_action(&api, &sign_in));
     if let Ok(v) = HeaderValue::from_str(&csp) {
         h.insert(header::CONTENT_SECURITY_POLICY, v);
     }
@@ -88,12 +91,12 @@ async fn guard(State(api): State<Api>, req: Request, next: Next) -> Response {
 }
 
 /// `303 See Other` to `to`.
-fn see_other(to: &str) -> Response {
+pub(crate) fn see_other(to: &str) -> Response {
     (StatusCode::SEE_OTHER, [(header::LOCATION, to)]).into_response()
 }
 
 /// An HTML page with `code`.
-fn html(code: StatusCode, page: String) -> Response {
+pub(crate) fn html(code: StatusCode, page: String) -> Response {
     (
         code,
         [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
@@ -118,13 +121,14 @@ pub(crate) fn safe_next(next: Option<&str>) -> String {
 }
 
 /// The query's parameters, the last of each name taken.
-fn params(raw: Option<&str>) -> BTreeMap<String, String> {
+pub(crate) fn params(raw: Option<&str>) -> BTreeMap<String, String> {
     url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
         .into_owned()
         .collect()
 }
 
-fn frame(api: &Api, sign_in: &SignIn) -> Frame {
+/// What every page shows around its card, for this node and these settings.
+pub(crate) fn frame(api: &Api, sign_in: &SignIn) -> Frame {
     Frame {
         node: api
             .node
@@ -135,14 +139,34 @@ fn frame(api: &Api, sign_in: &SignIn) -> Frame {
 }
 
 /// A page under `/ui/` (the app's, from M15): the landing page for a caller who verifies, the
-/// sign-in for one who brought nothing, else why not.
+/// sign-in for one who brought nothing, else why not. An `oauth2` session whose token is about
+/// to expire is refreshed first, its new cookies set with the page.
 async fn page(State(api): State<Api>, uri: Uri, headers: HeaderMap) -> Response {
     let Some(sign_in) = api.auth.sign_in() else {
         return StatusCode::NOT_FOUND.into_response();
     };
     let frame = frame(&api, &sign_in);
     let here = safe_next(uri.path_and_query().map(|p| p.as_str()));
-    match api.auth.verify_caller(&headers).await {
+    let mut headers = headers;
+    let mut renewed = Vec::new();
+    let mut not_refreshed = None;
+    match oauth2::due(&api, &sign_in, &headers).await {
+        Some(Ok(new)) => {
+            if let Some(cookies) = sign_in
+                .sessions
+                .as_ref()
+                .and_then(|s| session_cookies(&new, s).ok())
+            {
+                if let Ok(v) = as_sent(&cookies).parse() {
+                    headers.insert(header::COOKIE, v);
+                }
+                renewed = cookies;
+            }
+        }
+        Some(Err(e)) => not_refreshed = Some(e),
+        None => {}
+    }
+    let mut r = match api.auth.verify_caller(&headers).await {
         Ok(Verified { principal, session }) => html(
             StatusCode::OK,
             pages::signed_in(
@@ -152,8 +176,14 @@ async fn page(State(api): State<Api>, uri: Uri, headers: HeaderMap) -> Response
                 session.is_some(),
             ),
         ),
+        Err(Denied::Unauthenticated { .. }) if let Some(e) = not_refreshed => {
+            let why = format!("the provider's token expired and couldn't be refreshed: {e}");
+            html(StatusCode::UNAUTHORIZED, pages::ended(&frame, &why, &here))
+        }
         Err(denied) => refused(&frame, &sign_in, &headers, &here, denied),
-    }
+    };
+    set_cookies(&mut r, renewed);
+    r
 }
 
 /// Why a page isn't shown: the sign-in for one who brought no credentials (`basic`, `oauth2`),
@@ -186,7 +216,8 @@ fn refused(
     }
 }
 
-/// `GET /ui/sign-in`: the form (`basic`); a caller already signed in goes on to `next`.
+/// `GET /ui/sign-in`: the form (`basic`), or the way to the provider (`oauth2`); a caller
+/// already signed in goes on to `next`.
 async fn sign_in_page(
     State(api): State<Api>,
     RawQuery(raw): RawQuery,
@@ -207,23 +238,22 @@ async fn sign_in_page(
     {
         return see_other(&next);
     }
-    let frame = frame(&api, &sign_in);
     match sign_in.auth {
         ApiAuth::Basic => {
             let info = q.contains_key("signed_out").then_some("You signed out.");
             html(
                 StatusCode::OK,
-                pages::basic_sign_in(&frame, &next, "", None, info),
+                pages::basic_sign_in(&frame(&api, &sign_in), &next, "", None, info),
             )
         }
-        _ => StatusCode::NOT_FOUND.into_response(),
+        _ => oauth2::sign_in_page(&api, &sign_in, &q, &next),
     }
 }
 
 /// Whether a form posted to `/ui/` came from the console's own pages, as the browser says
 /// (`Sec-Fetch-Site`, else `Origin` against `Host`); a client that says neither (`curl`) is
 /// taken at its word.
-fn same_origin(headers: &HeaderMap) -> bool {
+pub(crate) fn same_origin(headers: &HeaderMap) -> bool {
     if let Some(site) = headers.get("sec-fetch-site") {
         return site == "same-origin" || site == "none";
     }
@@ -311,32 +341,45 @@ async fn sign_in(
         },
         Utc::now().timestamp(),
     );
-    started(&session, sessions, &next)
+    match session_cookies(&session, sessions) {
+        Ok(cookies) => {
+            let mut r = see_other(&next);
+            set_cookies(&mut r, cookies);
+            r
+        }
+        Err(e) => body(
+            StatusCode::INTERNAL_SERVER_ERROR,
+            &json!({ "error": e.to_string() }),
+        ),
+    }
 }
 
-/// A session started: its cookies, and on to `next`.
-pub(crate) fn started(session: &Session, sessions: &crate::auth::Sessions, next: &str) -> Response {
-    let max_age = (sessions.ttl + KEPT_AFTER).as_secs();
-    let cookies = match session.cookies(&sessions.sealer, max_age) {
-        Ok(c) => c,
-        Err(e) => {
-            return body(
-                StatusCode::INTERNAL_SERVER_ERROR,
-                &json!({ "error": e.to_string() }),
-            );
-        }
-    };
-    let mut r = see_other(next);
+/// The `Set-Cookie` values of `session`, kept until a day after it ends.
+///
+/// # Errors
+/// [`TooLong`]: it needs more cookies than a browser is sent.
+pub(crate) fn session_cookies(
+    session: &Session,
+    sessions: &Sessions,
+) -> Result<Vec<String>, TooLong> {
+    // In whole seconds: a session starts on one.
+    let left = sessions.ends(session.signed_in).timestamp() - Utc::now().timestamp();
+    let left = u64::try_from(left).unwrap_or_default();
+    session.cookies(&sessions.sealer, left + KEPT_AFTER.as_secs())
+}
+
+/// Adds `cookies` to `r`'s `Set-Cookie`s.
+pub(crate) fn set_cookies(r: &mut Response, cookies: impl IntoIterator<Item = String>) {
     for c in cookies {
         if let Ok(v) = HeaderValue::from_str(&c) {
             r.headers_mut().append(header::SET_COOKIE, v);
         }
     }
-    r
 }
 
-/// `POST /ui/sign-out`: the session's cookies removed, then the sign-in. Only from the
-/// console's own pages: a sign-out another site posts is refused.
+/// `POST /ui/sign-out`: the session's cookies removed, then the sign-in, or, with `oauth2`, the
+/// provider's end-session endpoint when it has one. Only from the console's own pages: a
+/// sign-out another site posts is refused.
 async fn sign_out(State(api): State<Api>, headers: HeaderMap) -> Response {
     let Some(sign_in) = api.auth.sign_in() else {
         return StatusCode::NOT_FOUND.into_response();
@@ -347,11 +390,11 @@ async fn sign_out(State(api): State<Api>, headers: HeaderMap) -> Response {
             pages::cross_site(&frame(&api, &sign_in)),
         );
     }
-    let mut r = see_other("/ui/sign-in?signed_out");
-    for c in Session::removed() {
-        if let Ok(v) = HeaderValue::from_str(&c) {
-            r.headers_mut().append(header::SET_COOKIE, v);
-        }
+    let to = oauth2::end_session(&api, &sign_in, &headers).await;
+    let mut r = see_other(to.as_deref().unwrap_or("/ui/sign-in?signed_out"));
+    set_cookies(&mut r, Session::removed());
+    if sign_in.auth == ApiAuth::Oauth2 {
+        set_cookies(&mut r, [oauth2::id_removed()]);
     }
     r
 }
@@ -363,17 +406,7 @@ async fn session(State(api): State<Api>, headers: HeaderMap) -> Response {
         return StatusCode::NOT_FOUND.into_response();
     };
     match api.auth.verify_caller(&headers).await {
-        Ok(Verified { principal, session }) => body(
-            StatusCode::OK,
-            &json!({
-                "name": principal.name,
-                "role": principal.role,
-                "auth": sign_in.auth,
-                "expires": principal.expires.map(|e| e.to_rfc3339_opts(SecondsFormat::Secs, true)),
-                "signed_in": session.as_ref().and_then(|s| chrono::DateTime::from_timestamp(s.signed_in, 0)).map(|t| t.to_rfc3339_opts(SecondsFormat::Secs, true)),
-                "csrf": session.map(|s| s.csrf),
-            }),
-        ),
+        Ok(v) => body(StatusCode::OK, &session_json(&sign_in, v)),
         Err(Denied::Unauthenticated { why, .. }) => {
             body(StatusCode::UNAUTHORIZED, &json!({ "error": why }))
         }
@@ -387,6 +420,23 @@ async fn session(State(api): State<Api>, headers: HeaderMap) -> Response {
     }
 }
 
+/// The caller as `GET /ui/session` describes them.
+pub(crate) fn session_json(sign_in: &SignIn, v: Verified) -> Value {
+    let at = |t: chrono::DateTime<Utc>| t.to_rfc3339_opts(SecondsFormat::Secs, true);
+    json!({
+        "name": v.principal.name,
+        "role": v.principal.role,
+        "auth": sign_in.auth,
+        "expires": v.principal.expires.map(at),
+        "signed_in": v
+            .session
+            .as_ref()
+            .and_then(|s| chrono::DateTime::from_timestamp(s.signed_in, 0))
+            .map(at),
+        "csrf": v.session.map(|s| s.csrf),
+    })
+}
+
 /// The pages' stylesheet and scripts, built into the binary.
 async fn asset(Path(file): Path<String>) -> Response {
     let (text, kind) = match file.as_str() {
@@ -394,6 +444,10 @@ async fn asset(Path(file): Path<String>) -> Response {
             include_str!("../assets/console.css"),
             "text/css; charset=utf-8",
         ),
+        "sign-in.js" => (
+            include_str!("../assets/sign-in.js"),
+            "text/javascript; charset=utf-8",
+        ),
         _ => return StatusCode::NOT_FOUND.into_response(),
     };
     (
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 0c07c11..1bcd968 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -68,6 +68,7 @@ pub use follow::{FOLLOW_SILENCE, MAX_FRAME};
 pub use history::{MAX_POINTS, MAX_SPAN};
 pub use limit::{RateLimit, VALIDATE_BURST, VALIDATE_PER_SECOND};
 pub use live::FRESH_WITHIN;
+pub use oauth2::{AUTO_REDIRECT, ID_COOKIE, REFRESH_AHEAD, SIGN_IN_COOKIE, SIGN_IN_WITHIN};
 pub use openapi::openapi;
 pub use peers::{
     ACTION_LIMIT, ANSWERED_BY, DETAIL_LIMIT, FORWARDED_BY, MAX_ANSWER, Peers, PeersError,
@@ -919,7 +920,7 @@ async fn misdirected(State(api): State<Api>, req: Request, next: Next) -> Respon
 
 /// The 403 of a request its session authenticates that changes something without the session's
 /// anti-forgery token.
-const FORGED: &str = "a request the console's session authenticates sends the session's anti-forgery token (X-CSRF-Token) when it changes something";
+pub(crate) const FORGED: &str = "a request the console's session authenticates sends the session's anti-forgery token (X-CSRF-Token) when it changes something";
 
 /// Whether a request its session authenticates sent the session's anti-forgery token, as a
 /// request that changes something must: a `GET`, a `HEAD` or an `OPTIONS` needs none.
````

`crates/data-avalanche-api/src/oauth2.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/oauth2.rs b/crates/data-avalanche-api/src/oauth2.rs
index 52135c3..9ea77d5 100644
--- a/crates/data-avalanche-api/src/oauth2.rs
+++ b/crates/data-avalanche-api/src/oauth2.rs
@@ -1,3 +1,794 @@
+//! The console's `oauth2` sign-in (Admin_Console §9; config §6.3.1): authorization code with
+//! PKCE (S256) against the cluster's provider, the code exchanged by the server with the
+//! console's client (`globals.api.oauth2.client_id`, and its secret for a confidential client);
+//! the session refreshed with the provider's refresh token, when it issued one; and the sign-out
+//! taken on to the provider's end-session endpoint, when it has one. The endpoints come from the
+//! provider's OIDC discovery document (`discovery_url`, else the issuer's
+//! `/.well-known/openid-configuration`), whose `issuer` must be the configured one.
+
+use std::collections::{BTreeMap, BTreeSet, HashMap};
+use std::sync::{Mutex, PoisonError};
+use std::time::{Duration, Instant};
+
+use aws_lc_rs::digest::{SHA256, digest};
+use axum::extract::{RawQuery, State};
+use axum::http::{HeaderMap, Method, StatusCode, header};
+use axum::response::{IntoResponse, Response};
+use base64::Engine;
+use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
+use chrono::Utc;
+use data_avalanche_config::{ApiAuth, ApiOauth2};
+use serde::{Deserialize, Serialize};
+use serde_json::{Value, json};
+
+use crate::auth::{Denied, FETCH_TIMEOUT, MAX_KEY_DOCUMENT, Sessions, SignIn};
+use crate::console::{
+    frame, html, params, safe_next, same_origin, see_other, session_cookies, session_json,
+    set_cookies,
+};
+use crate::pages;
+use crate::session::{By, KEPT_AFTER, Session, as_sent, cookie, random_token, removed, set_cookie};
+use crate::{Api, body};
+
+/// How long a sign-in may stay at the provider: the state it left with lasts this long.
+pub const SIGN_IN_WITHIN: Duration = Duration::from_secs(600);
+/// An `oauth2` session's access token is refreshed once it expires within this long.
+pub const REFRESH_AHEAD: Duration = Duration::from_secs(60);
+/// The sign-in page goes to the provider by itself after this long, unless the person stays
+/// (Admin_Console §9).
+pub const AUTO_REDIRECT: Duration = Duration::from_secs(5);
+/// The provider's discovery document is read again after this long.
+const ENDPOINTS_MAX_AGE: Duration = Duration::from_secs(600);
+/// A refresh's outcome is given again, for this long, to another request that brings the same
+/// refresh token: a provider that rotates refresh tokens refuses one used twice, and may end
+/// the whole sign-in for it.
+const REFRESHED_KEPT: Duration = Duration::from_secs(30);
+
+/// The provider's endpoints the console uses.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub(crate) struct Endpoints {
+    pub authorization: String,
+    pub token: String,
+    pub end_session: Option<String>,
+    /// The client authenticates with HTTP Basic (`client_secret_basic`, the default), else in
+    /// the form (`client_secret_post`), as the provider says it takes.
+    pub secret_in_form: bool,
+}
+
+/// What the token endpoint answered.
+#[derive(Clone, PartialEq, Eq)]
+pub(crate) struct Tokens {
+    pub access: String,
+    pub refresh: Option<String>,
+    pub id: Option<String>,
+}
+
+impl std::fmt::Debug for Tokens {
+    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
+        f.debug_struct("Tokens").finish_non_exhaustive()
+    }
+}
+
+/// When a refresh was made, and what it gave.
+type Refreshed = (Instant, Result<Tokens, String>);
+
+/// The client of the cluster's provider: its endpoints cached, and the refreshes just made.
+#[derive(Debug)]
+pub(crate) struct Provider {
+    http: reqwest::Client,
+    /// The discovery URL read, its endpoints, and when.
+    endpoints: Mutex<Option<(String, Endpoints, Instant)>>,
+    /// A refresh token's digest, and what its refresh gave.
+    refreshed: Mutex<HashMap<[u8; 32], Refreshed>>,
+    /// Held through a refresh: two requests that bring the same refresh token at once make one.
+    refreshing: tokio::sync::Mutex<()>,
+}
+
+impl Default for Provider {
+    fn default() -> Self {
+        Self {
+            http: reqwest::Client::builder()
+                .timeout(FETCH_TIMEOUT)
+                // The provider's URLs are the configured and discovered ones (DA-API-16).
+                .redirect(reqwest::redirect::Policy::none())
+                .build()
+                .expect("the HTTP client builds"),
+            endpoints: Mutex::default(),
+            refreshed: Mutex::default(),
+            refreshing: tokio::sync::Mutex::default(),
+        }
+    }
+}
+
+/// The discovery document of `o`.
+fn discovery_url(o: &ApiOauth2) -> String {
+    o.discovery_url.clone().unwrap_or_else(|| {
+        format!(
+            "{}/.well-known/openid-configuration",
+            o.issuer.trim_end_matches('/')
+        )
+    })
+}
+
+/// A PKCE verifier's challenge (RFC 7636 §4.2, `S256`).
+pub(crate) fn challenge(verifier: &str) -> String {
+    URL_SAFE_NO_PAD.encode(digest(&SHA256, verifier.as_bytes()).as_ref())
+}
+
+/// The origin of `url`, as a Content-Security-Policy source.
+pub(crate) fn origin(url: &str) -> Option<String> {
+    let u = url::Url::parse(url).ok()?;
+    Some(u.origin().ascii_serialization()).filter(|o| o != "null")
+}
+
+/// The seconds until the JWT `token` expires, read without verifying it (only to decide when to
+/// refresh: the new token is verified as any is); `None` when it can't be read.
+pub(crate) fn expires_in(token: &str, now: i64) -> Option<i64> {
+    let payload = token.split('.').nth(1)?;
+    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()?;
+    Some(claims.get("exp")?.as_i64()? - now)
+}
+
+/// `form` as the console's client posts it to the token endpoint, and its `Authorization`: a
+/// confidential client's secret goes in HTTP Basic (`client_secret_basic`, RFC 6749 §2.3.1),
+/// or in the form when the provider takes only that (`client_secret_post`); a public client
+/// names itself in the form.
+fn client_form(o: &ApiOauth2, e: &Endpoints, form: &[(&str, &str)]) -> (String, Option<String>) {
+    let client_id = o.client_id.as_deref().unwrap_or_default();
+    let mut body = url::form_urlencoded::Serializer::new(String::new());
+    body.extend_pairs(form);
+    let mut basic = None;
+    match &o.client_secret {
+        Some(secret) if e.secret_in_form => {
+            body.append_pair("client_id", client_id)
+                .append_pair("client_secret", secret.expose());
+        }
+        Some(secret) => {
+            // Each form-encoded, then joined by `:`.
+            let enc =
+                |s: &str| url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>();
+            let pair = format!("{}:{}", enc(client_id), enc(secret.expose()));
+            basic = Some(format!("Basic {}", STANDARD.encode(pair)));
+        }
+        None => {
+            body.append_pair("client_id", client_id);
+        }
+    }
+    (body.finish(), basic)
+}
+
+impl Provider {
+    /// The endpoints of `o`'s provider: discovered, then cached for [`ENDPOINTS_MAX_AGE`].
+    ///
+    /// # Errors
+    /// The discovery document can't be read, names another issuer, or lacks an endpoint.
+    pub(crate) async fn endpoints(&self, o: &ApiOauth2) -> Result<Endpoints, String> {
+        let url = discovery_url(o);
+        if let Some((at, e, when)) = &*self
+            .endpoints
+            .lock()
+            .unwrap_or_else(PoisonError::into_inner)
+            && *at == url
+            && when.elapsed() < ENDPOINTS_MAX_AGE
+        {
+            return Ok(e.clone());
+        }
+        let doc: Value = serde_json::from_slice(&self.read(self.http.get(&url)).await?)
+            .map_err(|e| format!("{url}: {e}"))?;
+        let found = doc.get("issuer").and_then(Value::as_str);
+        if found != Some(o.issuer.as_str()) {
+            return Err(format!(
+                "{url}: the issuer is {found:?}, not the configured {:?}",
+                o.issuer
+            ));
+        }
+        let text = |name: &str| doc.get(name).and_then(Value::as_str).map(str::to_owned);
+        let methods: Vec<&str> = doc
+            .get("token_endpoint_auth_methods_supported")
+            .and_then(Value::as_array)
+            .map(|m| m.iter().filter_map(Value::as_str).collect())
+            .unwrap_or_default();
+        let e = Endpoints {
+            authorization: text("authorization_endpoint")
+                .ok_or_else(|| format!("{url}: no authorization_endpoint"))?,
+            token: text("token_endpoint").ok_or_else(|| format!("{url}: no token_endpoint"))?,
+            end_session: text("end_session_endpoint"),
+            secret_in_form: !methods.is_empty()
+                && !methods.contains(&"client_secret_basic")
+                && methods.contains(&"client_secret_post"),
+        };
+        *self
+            .endpoints
+            .lock()
+            .unwrap_or_else(PoisonError::into_inner) = Some((url, e.clone(), Instant::now()));
+        Ok(e)
+    }
+
+    /// The endpoints cached for `o`, if any: what the Content-Security-Policy reads.
+    pub(crate) fn cached(&self, o: &ApiOauth2) -> Option<Endpoints> {
+        let url = discovery_url(o);
+        self.endpoints
+            .lock()
+            .unwrap_or_else(PoisonError::into_inner)
+            .as_ref()
+            .filter(|(at, _, _)| *at == url)
+            .map(|(_, e, _)| e.clone())
+    }
+
+    /// The body of `req`'s answer: a redirect isn't followed, an answer other than `200` is an
+    /// error with the provider's `error_description` (or `error`), and a body past
+    /// [`MAX_KEY_DOCUMENT`] isn't read.
+    async fn read(&self, req: reqwest::RequestBuilder) -> Result<Vec<u8>, String> {
+        let mut r = req.send().await.map_err(|e| crate::peers::cause(&e))?;
+        let code = r.status();
+        let mut body = Vec::new();
+        while let Some(chunk) = r.chunk().await.map_err(|e| crate::peers::cause(&e))? {
+            if body.len() + chunk.len() > MAX_KEY_DOCUMENT {
+                return Err(format!(
+                    "the provider's answer is longer than {MAX_KEY_DOCUMENT} bytes"
+                ));
+            }
+            body.extend_from_slice(&chunk);
+        }
+        if code != reqwest::StatusCode::OK {
+            let said: Option<String> = serde_json::from_slice::<Value>(&body).ok().and_then(|v| {
+                v.get("error_description")
+                    .or_else(|| v.get("error"))
+                    .and_then(Value::as_str)
+                    .map(|s| crate::actions::clipped(s, crate::MAX_ERROR).into_owned())
+            });
+            return Err(match said {
+                Some(s) => format!("the provider answered {}: {s}", code.as_u16()),
+                None => format!("the provider answered {}", code.as_u16()),
+            });
+        }
+        Ok(body)
+    }
+
+    /// Posts `form` to the token endpoint as the console's client.
+    async fn token(
+        &self,
+        o: &ApiOauth2,
+        e: &Endpoints,
+        form: &[(&str, &str)],
+    ) -> Result<Tokens, String> {
+        let (form, basic) = client_form(o, e, form);
+        let mut req = self
+            .http
+            .post(&e.token)
+            .header(reqwest::header::ACCEPT, "application/json")
+            .header(
+                reqwest::header::CONTENT_TYPE,
+                "application/x-www-form-urlencoded",
+            );
+        if let Some(basic) = basic {
+            req = req.header(reqwest::header::AUTHORIZATION, basic);
+        }
+        let answer = self.read(req.body(form)).await?;
+        let v: Value = serde_json::from_slice(&answer)
+            .map_err(|e| format!("the provider's token answer: {e}"))?;
+        let text = |name: &str| v.get(name).and_then(Value::as_str).map(str::to_owned);
+        Ok(Tokens {
+            access: text("access_token")
+                .ok_or("the provider's token answer has no access_token")?,
+            refresh: text("refresh_token"),
+            id: text("id_token"),
+        })
+    }
+
+    /// Exchanges a sign-in's `code` (RFC 6749 §4.1.3, with the PKCE `verifier`).
+    ///
+    /// # Errors
+    /// The provider refused it, or answered what isn't a token answer.
+    pub(crate) async fn exchange(
+        &self,
+        o: &ApiOauth2,
+        e: &Endpoints,
+        code: &str,
+        verifier: &str,
+        redirect_uri: &str,
+    ) -> Result<Tokens, String> {
+        self.token(
+            o,
+            e,
+            &[
+                ("grant_type", "authorization_code"),
+                ("code", code),
+                ("redirect_uri", redirect_uri),
+                ("code_verifier", verifier),
+            ],
+        )
+        .await
+    }
+
+    /// Refreshes with `refresh` (RFC 6749 §6); a provider that issues no new refresh token keeps
+    /// the old one. Another request bringing the same refresh token within
+    /// [`REFRESHED_KEPT`] gets this one's outcome, without asking the provider again.
+    ///
+    /// # Errors
+    /// The provider refused it.
+    pub(crate) async fn refresh(
+        &self,
+        o: &ApiOauth2,
+        e: &Endpoints,
+        refresh: &str,
+    ) -> Result<Tokens, String> {
+        let key = *blake3::hash(refresh.as_bytes()).as_bytes();
+        let _one_at_a_time = self.refreshing.lock().await;
+        {
+            let mut done = self
+                .refreshed
+                .lock()
+                .unwrap_or_else(PoisonError::into_inner);
+            done.retain(|_, (at, _)| at.elapsed() < REFRESHED_KEPT);
+            if let Some((_, outcome)) = done.get(&key) {
+                return outcome.clone();
+            }
+        }
+        let outcome = self
+            .token(
+                o,
+                e,
+                &[("grant_type", "refresh_token"), ("refresh_token", refresh)],
+            )
+            .await
+            .map(|mut t| {
+                t.refresh = t.refresh.or_else(|| Some(refresh.to_owned()));
+                t
+            });
+        self.refreshed
+            .lock()
+            .unwrap_or_else(PoisonError::into_inner)
+            .insert(key, (Instant::now(), outcome.clone()));
+        outcome
+    }
+}
+
+/// The cookie a sign-in leaves for the provider with: its state, PKCE verifier and `next`,
+/// sealed. Sent back only to the callback, and `SameSite=Lax`: the provider's redirect back is
+/// another site's navigation, which a `Strict` cookie doesn't follow.
+pub const SIGN_IN_COOKIE: &str = "da_oauth2";
+/// The cookie that keeps the provider's ID token, sealed, for the sign-out's `id_token_hint`:
+/// sent only to `/ui/sign-out`.
+pub const ID_COOKIE: &str = "da_id";
+/// The callback, under the console's `public_url`: the redirect URI the provider registered.
+const CALLBACK: &str = "/ui/oauth2/callback";
+/// The sign-out, the only path the ID token's cookie is sent to.
+const SIGN_OUT: &str = "/ui/sign-out";
+
+/// What a sign-in left with, in [`SIGN_IN_COOKIE`].
+#[derive(Serialize, Deserialize)]
+struct Started {
+    state: String,
+    verifier: String,
+    next: String,
+    /// When it began, Unix seconds.
+    at: i64,
+}
+
+/// What the `oauth2` routes read: the provider's settings, the sessions, the redirect URI.
+pub(crate) struct Client<'s> {
+    pub o: &'s ApiOauth2,
+    pub sessions: &'s Sessions,
+    pub public_url: &'s str,
+}
+
+impl<'s> Client<'s> {
+    /// The console's client, with `auth: oauth2` and what it reads (V-135, V-136).
+    pub(crate) fn of(sign_in: &'s SignIn) -> Option<Self> {
+        match (
+            sign_in.auth,
+            &sign_in.oauth2,
+            &sign_in.sessions,
+            &sign_in.public_url,
+        ) {
+            (ApiAuth::Oauth2, Some(o), Some(sessions), Some(url)) if o.client_id.is_some() => {
+                Some(Self {
+                    o,
+                    sessions,
+                    public_url: url.trim_end_matches('/'),
+                })
+            }
+            _ => None,
+        }
+    }
+
+    fn redirect_uri(&self) -> String {
+        format!("{}{CALLBACK}", self.public_url)
+    }
+
+    /// The provider as the sign-in page names it: `idp_name`, else the issuer's host.
+    pub(crate) fn name(&self) -> String {
+        self.o.idp_name.clone().unwrap_or_else(|| {
+            url::Url::parse(&self.o.issuer)
+                .ok()
+                .and_then(|u| u.host_str().map(str::to_owned))
+                .unwrap_or_else(|| self.o.issuer.clone())
+        })
+    }
+}
+
+/// `form-action`'s other sources: the provider's origins a sign-out form goes on to (the
+/// issuer's, and its end-session endpoint's once discovered).
+pub(crate) fn form_action(api: &Api, sign_in: &SignIn) -> String {
+    let Some(c) = Client::of(sign_in) else {
+        return String::new();
+    };
+    let mut origins: BTreeSet<String> = origin(&c.o.issuer).into_iter().collect();
+    if let Some(end) = api
+        .auth
+        .provider
+        .cached(c.o)
+        .and_then(|e| e.end_session)
+        .and_then(|e| origin(&e))
+    {
+        origins.insert(end);
+    }
+    origins.iter().map(|o| format!(" {o}")).collect()
+}
+
+/// The `oauth2` sign-in page: it goes to the provider by itself after [`AUTO_REDIRECT`], unless
+/// it follows a sign-out or a failed sign-in, or the person chose to stay (no loop).
+pub(crate) fn sign_in_page(
+    api: &Api,
+    sign_in: &SignIn,
+    q: &BTreeMap<String, String>,
+    next: &str,
+) -> Response {
+    let Some(c) = Client::of(sign_in) else {
+        return StatusCode::NOT_FOUND.into_response();
+    };
+    let stays = ["signed_out", "failed", "stay"]
+        .iter()
+        .any(|k| q.contains_key(*k));
+    let info = q.contains_key("signed_out").then_some("You signed out.");
+    html(
+        StatusCode::OK,
+        pages::oauth2_sign_in(
+            &frame(api, sign_in),
+            next,
+            &c.name(),
+            (!stays).then_some(AUTO_REDIRECT.as_secs()),
+            None,
+            info,
+        ),
+    )
+}
+
+/// A failed sign-in: the sign-in page again, saying why, without the countdown.
+fn failed(
+    api: &Api,
+    sign_in: &SignIn,
+    c: &Client<'_>,
+    code: StatusCode,
+    next: &str,
+    why: &str,
+) -> Response {
+    let mut r = html(
+        code,
+        pages::oauth2_sign_in(&frame(api, sign_in), next, &c.name(), None, Some(why), None),
+    );
+    set_cookies(&mut r, [removed(SIGN_IN_COOKIE, CALLBACK)]);
+    r
+}
+
+/// `GET /ui/oauth2/start`: to the provider's authorization endpoint (authorization code, PKCE
+/// `S256`, a new state), what came back checked against [`SIGN_IN_COOKIE`].
+pub(crate) async fn start(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
+    let Some(sign_in) = api.auth.sign_in() else {
+        return StatusCode::NOT_FOUND.into_response();
+    };
+    let Some(c) = Client::of(&sign_in) else {
+        return StatusCode::NOT_FOUND.into_response();
+    };
+    let next = safe_next(params(raw.as_deref()).get("next").map(String::as_str));
+    let endpoints = match api.auth.provider.endpoints(c.o).await {
+        Ok(e) => e,
+        Err(e) => {
+            let why = format!("{} can't be reached: {e}", c.name());
+            return failed(
+                &api,
+                &sign_in,
+                &c,
+                StatusCode::SERVICE_UNAVAILABLE,
+                &next,
+                &why,
+            );
+        }
+    };
+    let started = Started {
+        state: random_token(),
+        verifier: random_token(),
+        next,
+        at: Utc::now().timestamp(),
+    };
+    let to = url::Url::parse(&endpoints.authorization).map_or_else(
+        |_| endpoints.authorization.clone(),
+        |mut u| {
+            u.query_pairs_mut()
+                .append_pair("response_type", "code")
+                .append_pair("client_id", c.o.client_id.as_deref().unwrap_or_default())
+                .append_pair("redirect_uri", &c.redirect_uri())
+                .append_pair("scope", &c.o.scopes.join(" "))
+                .append_pair("state", &started.state)
+                .append_pair("code_challenge", &challenge(&started.verifier))
+                .append_pair("code_challenge_method", "S256");
+            u.to_string()
+        },
+    );
+    let sealed = c.sessions.sealer.seal(
+        "oauth2",
+        &serde_json::to_vec(&started).expect("a sign-in serializes"),
+    );
+    let mut r = see_other(&to);
+    set_cookies(
+        &mut r,
+        [set_cookie(
+            SIGN_IN_COOKIE,
+            &sealed,
+            CALLBACK,
+            SIGN_IN_WITHIN.as_secs(),
+            "Lax",
+        )],
+    );
+    r
+}
+
+/// `GET /ui/oauth2/callback`: the sign-in checked against what it left with, its code
+/// exchanged, the provider's token verified as the API verifies one, and a session started. A
+/// token with no role this node maps is "Forbidden" at once, naming the roles it carried.
+pub(crate) async fn callback(
+    State(api): State<Api>,
+    RawQuery(raw): RawQuery,
+    headers: HeaderMap,
+) -> Response {
+    let Some(sign_in) = api.auth.sign_in() else {
+        return StatusCode::NOT_FOUND.into_response();
+    };
+    let Some(c) = Client::of(&sign_in) else {
+        return StatusCode::NOT_FOUND.into_response();
+    };
+    let q = params(raw.as_deref());
+    let started: Option<Started> = cookie(&headers, SIGN_IN_COOKIE)
+        .and_then(|v| c.sessions.sealer.open("oauth2", v).ok())
+        .and_then(|plain| serde_json::from_slice(&plain).ok())
+        .filter(|s: &Started| {
+            Utc::now().timestamp() - s.at
+                < i64::try_from(SIGN_IN_WITHIN.as_secs()).unwrap_or(i64::MAX)
+        });
+    let Some(started) = started else {
+        let why = format!(
+            "The sign-in took more than {}, or began in another browser: sign in again.",
+            crate::auth::span(SIGN_IN_WITHIN)
+        );
+        return failed(&api, &sign_in, &c, StatusCode::BAD_REQUEST, "/ui/", &why);
+    };
+    let fail = |code, why: &str| failed(&api, &sign_in, &c, code, &started.next, why);
+    if let Some(e) = q.get("error") {
+        let said = q.get("error_description").unwrap_or(e);
+        let why = format!(
+            "{} refused the sign-in: {}",
+            c.name(),
+            crate::actions::clipped(said, crate::MAX_ERROR)
+        );
+        return fail(StatusCode::BAD_REQUEST, &why);
+    }
+    let state = q.get("state").map_or("", String::as_str);
+    let (Some(code), true) = (
+        q.get("code"),
+        crate::auth::same_secret(state, &started.state),
+    ) else {
+        return fail(
+            StatusCode::BAD_REQUEST,
+            "The provider's answer isn't this sign-in's: sign in again.",
+        );
+    };
+    let tokens = match api.auth.provider.endpoints(c.o).await {
+        Ok(e) => {
+            api.auth
+                .provider
+                .exchange(c.o, &e, code, &started.verifier, &c.redirect_uri())
+                .await
+        }
+        Err(e) => Err(e),
+    };
+    let tokens = match tokens {
+        Ok(t) => t,
+        Err(e) => {
+            let why = format!("{} didn't exchange the sign-in's code: {e}", c.name());
+            return fail(StatusCode::BAD_GATEWAY, &why);
+        }
+    };
+    match api.auth.verify_token(&tokens.access).await {
+        Ok(_) => {}
+        Err(Denied::Forbidden { name, why }) => {
+            let mut r = html(
+                StatusCode::FORBIDDEN,
+                pages::forbidden(&frame(&api, &sign_in), &name, &why, false),
+            );
+            set_cookies(&mut r, [removed(SIGN_IN_COOKIE, CALLBACK)]);
+            return r;
+        }
+        Err(Denied::Unauthenticated { why, .. } | Denied::Unavailable(why)) => {
+            let why = format!("The provider's token didn't verify: {why}.");
+            return fail(StatusCode::UNAUTHORIZED, &why);
+        }
+    }
+    let session = Session::new(
+        By::Oauth2 {
+            access: tokens.access,
+            refresh: tokens.refresh,
+        },
+        Utc::now().timestamp(),
+    );
+    let cookies = match session_cookies(&session, c.sessions) {
+        Ok(cookies) => cookies,
+        Err(e) => return fail(StatusCode::BAD_GATEWAY, &e.to_string()),
+    };
+    let mut r = html(
+        StatusCode::OK,
+        pages::signing_in(&frame(&api, &sign_in), &started.next),
+    );
+    set_cookies(&mut r, cookies);
+    set_cookies(&mut r, [removed(SIGN_IN_COOKIE, CALLBACK)]);
+    let id = tokens
+        .id
+        .map(|id| c.sessions.sealer.seal("id_token", id.as_bytes()))
+        .filter(|sealed| sealed.len() <= crate::COOKIE_CHUNK);
+    if let Some(sealed) = id {
+        let max_age = (c.sessions.ttl + KEPT_AFTER).as_secs();
+        set_cookies(
+            &mut r,
+            [set_cookie(ID_COOKIE, &sealed, SIGN_OUT, max_age, "Strict")],
+        );
+    }
+    r
+}
+
+/// `session` with its access token refreshed; it keeps when it signed in and its anti-forgery
+/// token, so it ends when it would have.
+///
+/// # Errors
+/// It has no refresh token, or the provider refused it.
+pub(crate) async fn renewed(
+    api: &Api,
+    c: &Client<'_>,
+    session: &Session,
+) -> Result<Session, String> {
+    let By::Oauth2 {
+        refresh: Some(refresh),
+        ..
+    } = &session.by
+    else {
+        return Err("the provider issued no refresh token".into());
+    };
+    let e = api.auth.provider.endpoints(c.o).await?;
+    let t = api.auth.provider.refresh(c.o, &e, refresh).await?;
+    Ok(Session {
+        by: By::Oauth2 {
+            access: t.access,
+            refresh: t.refresh,
+        },
+        ..session.clone()
+    })
+}
+
+/// The refresh a page's request makes: an `oauth2` session, not ended, whose access token
+/// expires within [`REFRESH_AHEAD`] and which has a refresh token, refreshed. `None`: nothing to
+/// refresh.
+pub(crate) async fn due(
+    api: &Api,
+    sign_in: &SignIn,
+    headers: &HeaderMap,
+) -> Option<Result<Session, String>> {
+    let c = Client::of(sign_in)?;
+    let session = Session::of(headers, &c.sessions.sealer)?.ok()?;
+    let By::Oauth2 {
+        access,
+        refresh: Some(_),
+    } = &session.by
+    else {
+        return None;
+    };
+    let now = Utc::now();
+    if now >= c.sessions.ends(session.signed_in) {
+        return None;
+    }
+    let left = expires_in(access, now.timestamp())?;
+    if left > i64::try_from(REFRESH_AHEAD.as_secs()).unwrap_or(i64::MAX) {
+        return None;
+    }
+    Some(renewed(api, &c, &session).await)
+}
+
+/// `POST /ui/session/refresh`: the console's app refreshes its session ahead of its token's
+/// expiry, sending the session's anti-forgery token; it answers as `GET /ui/session` does, with
+/// the new cookies.
+pub(crate) async fn refresh(State(api): State<Api>, headers: HeaderMap) -> Response {
+    let Some(sign_in) = api.auth.sign_in() else {
+        return StatusCode::NOT_FOUND.into_response();
+    };
+    let Some(c) = Client::of(&sign_in) else {
+        return StatusCode::NOT_FOUND.into_response();
+    };
+    let unauthorized = |why: String| body(StatusCode::UNAUTHORIZED, &json!({ "error": why }));
+    let session = match Session::of(&headers, &c.sessions.sealer) {
+        None => return unauthorized("not signed in".into()),
+        Some(Err(e)) => return unauthorized(e.to_string()),
+        Some(Ok(s)) => s,
+    };
+    if !same_origin(&headers) || !crate::unforged(&Method::POST, &headers, &session.csrf) {
+        return body(StatusCode::FORBIDDEN, &json!({ "error": crate::FORGED }));
+    }
+    if !matches!(
+        session.by,
+        By::Oauth2 {
+            refresh: Some(_),
+            ..
+        }
+    ) {
+        return body(
+            StatusCode::CONFLICT,
+            &json!({ "error": "the provider issued no refresh token: sign in again once the session ends" }),
+        );
+    }
+    let new = match renewed(&api, &c, &session).await {
+        Ok(s) => s,
+        Err(e) => return unauthorized(format!("the provider's token couldn't be refreshed: {e}")),
+    };
+    let cookies = match session_cookies(&new, c.sessions) {
+        Ok(cookies) => cookies,
+        Err(e) => return unauthorized(e.to_string()),
+    };
+    let mut sent = headers.clone();
+    sent.insert(
+        header::COOKIE,
+        as_sent(&cookies).parse().expect("cookies are header text"),
+    );
+    let mut r = match api.auth.verify_caller(&sent).await {
+        Ok(v) => body(StatusCode::OK, &session_json(&sign_in, v)),
+        Err(Denied::Forbidden { why, name }) => body(
+            StatusCode::FORBIDDEN,
+            &json!({ "error": why, "name": name }),
+        ),
+        Err(Denied::Unauthenticated { why, .. } | Denied::Unavailable(why)) => unauthorized(why),
+    };
+    set_cookies(&mut r, cookies);
+    r
+}
+
+/// Where a sign-out goes on to: the provider's end-session endpoint, when it has one, with the
+/// console's client, the ID token kept, and the way back to the sign-in.
+pub(crate) async fn end_session(
+    api: &Api,
+    sign_in: &SignIn,
+    headers: &HeaderMap,
+) -> Option<String> {
+    let c = Client::of(sign_in)?;
+    let end = api.auth.provider.endpoints(c.o).await.ok()?.end_session?;
+    let mut u = url::Url::parse(&end).ok()?;
+    {
+        let mut q = u.query_pairs_mut();
+        q.append_pair("client_id", c.o.client_id.as_deref().unwrap_or_default())
+            .append_pair(
+                "post_logout_redirect_uri",
+                &format!("{}/ui/sign-in?signed_out", c.public_url),
+            );
+        let hint = cookie(headers, ID_COOKIE)
+            .and_then(|v| c.sessions.sealer.open("id_token", v).ok())
+            .and_then(|plain| String::from_utf8(plain).ok());
+        if let Some(hint) = hint {
+            q.append_pair("id_token_hint", &hint);
+        }
+    }
+    Some(u.to_string())
+}
+
+/// The sign-out's removal of the ID token's cookie.
+pub(crate) fn id_removed() -> String {
+    removed(ID_COOKIE, SIGN_OUT)
+}
+
 #[cfg(test)]
 mod tests {
     use std::net::SocketAddr;
````

`crates/data-avalanche-api/src/pages.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/pages.rs b/crates/data-avalanche-api/src/pages.rs
index 660f2ea..8abc38e 100644
--- a/crates/data-avalanche-api/src/pages.rs
+++ b/crates/data-avalanche-api/src/pages.rs
@@ -32,6 +32,11 @@ pub(crate) fn escape(s: &str) -> String {
 /// A whole page: `title` in the browser's tab, `card` (HTML) in the middle, `script` loaded
 /// after it when set.
 fn page(frame: &Frame, title: &str, card: &str, script: Option<&str>) -> String {
+    page_with(frame, title, card, "", script)
+}
+
+/// [`page`], with `head` (HTML) in its head.
+fn page_with(frame: &Frame, title: &str, card: &str, head: &str, script: Option<&str>) -> String {
     let banner = if frame.open {
         r#"<div class="warnband" role="alert">⚠ This cluster has no authentication: anyone who can reach it is an admin. Set <code>globals.api.auth</code>.</div>"#
     } else {
@@ -48,7 +53,7 @@ fn page(frame: &Frame, title: &str, card: &str, script: Option<&str>) -> String
 <meta name="viewport" content="width=device-width, initial-scale=1">
 <title>{title} · data-avalanche</title>
 <link rel="stylesheet" href="/ui/assets/console.css">
-</head>
+{head}</head>
 <body>
 {banner}<main><div class="card">
 <div class="brand">data-avalanche<small>console</small></div>
@@ -99,6 +104,63 @@ pub(crate) fn basic_sign_in(
     page(frame, "Sign in", &card, None)
 }
 
+/// The `oauth2` sign-in: a button to the provider, named by `provider`; with `countdown`, the
+/// page goes there by itself after that many seconds unless the person stays (its script).
+/// `error` and `info` are shown above the button.
+pub(crate) fn oauth2_sign_in(
+    frame: &Frame,
+    next: &str,
+    provider: &str,
+    countdown: Option<u64>,
+    error: Option<&str>,
+    info: Option<&str>,
+) -> String {
+    let next_q = escape(&encode(next));
+    let mut card = String::from(r#"<div class="muted">Sign in to the cluster</div>"#);
+    if let Some(e) = error {
+        let _ = write!(
+            card,
+            r#"<div class="msg err" role="alert">{}</div>"#,
+            escape(e)
+        );
+    }
+    if let Some(i) = info {
+        let _ = write!(card, r#"<div class="msg info">{}</div>"#, escape(i));
+    }
+    let _ = write!(
+        card,
+        r#"
+<a class="btn" id="continue" href="/ui/oauth2/start?next={next_q}">Continue with {provider}</a>"#,
+        provider = escape(provider),
+    );
+    if let Some(seconds) = countdown {
+        let _ = write!(
+            card,
+            r#"
+<div id="countdown" data-seconds="{seconds}"><p class="muted">Going to {provider} in <span id="left">{seconds}</span> s · <a id="stay" href="/ui/sign-in?next={next_q}&amp;stay">stay here</a></p><div class="count"><i id="bar"></i></div></div>"#,
+            provider = escape(provider),
+        );
+    }
+    page(frame, "Sign in", &card, Some("/ui/assets/sign-in.js"))
+}
+
+/// The OAuth2 callback's answer: the session's cookies are set, and the page goes on to `next`
+/// from here, the console's own page, so the browser sends them (a `SameSite=Strict` cookie
+/// isn't sent on a redirect the provider began).
+pub(crate) fn signing_in(frame: &Frame, next: &str) -> String {
+    let card = format!(
+        r#"<div class="title">Signed in</div>
+<p><a href="{next}">Go on to <code>{next}</code></a>.</p>"#,
+        next = escape(next),
+    );
+    let head = format!(
+        r#"<meta http-equiv="refresh" content="0;url={}">
+"#,
+        escape(next)
+    );
+    page_with(frame, "Signing in", &card, &head, None)
+}
+
 /// 401 with a session (`basic`, `oauth2`): it ended, why, and a way back to `next`.
 pub(crate) fn ended(frame: &Frame, why: &str, next: &str) -> String {
     let card = format!(
````

`crates/data-avalanche-api/src/session.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/session.rs b/crates/data-avalanche-api/src/session.rs
index c61c6b4..238c016 100644
--- a/crates/data-avalanche-api/src/session.rs
+++ b/crates/data-avalanche-api/src/session.rs
@@ -254,6 +254,16 @@ impl Session {
     }
 }
 
+/// The `Cookie` header a browser sends back after these `Set-Cookie` values: the ones they
+/// keep, each `name=value`.
+pub(crate) fn as_sent(set: &[String]) -> String {
+    set.iter()
+        .filter(|c| !c.contains("Max-Age=0"))
+        .filter_map(|c| c.split(';').next())
+        .collect::<Vec<_>>()
+        .join("; ")
+}
+
 /// The session's cookies, joined in order: `None` without its first.
 pub(crate) fn sealed_value(headers: &HeaderMap) -> Option<String> {
     let mut sealed = String::new();
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-api --lib -- oauth2:: console:: auth::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/assets/sign-in.js crates/data-avalanche-api/src/auth.rs crates/data-avalanche-api/src/console.rs crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/oauth2.rs crates/data-avalanche-api/src/pages.rs crates/data-avalanche-api/src/session.rs crates/data-avalanche-api/src/test_support.rs
git commit -m "feat(api): the console's oauth2 sign-in: PKCE, the callback, refresh, sign-out at the provider"
```

---

### Task 5: The site and the research docs

**Files:**
- Modify: `crates/data-avalanche-config/src/field_docs.rs`
- Modify: `doc/research/API_RBAC_and_Versioning.md`
- Modify: `doc/research/Admin_Console.md`
- Create: `docs/src/content/docs/deployment/console.md`
- Regenerate: `docs/src/content/docs/reference/configuration.md`

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces: the deployment page "The admin console" (`deployment/console.md`); `session_ttl`'s
  reference text (from `field_docs.rs`); `Admin_Console.md` §9.1 (the sessions as built);
  `API_RBAC_and_Versioning.md` §3 (the session beside the modes).

- [ ] **Step 1: Write the pages**

The console page says how to turn the console on, how people sign in under each mode, what a
session is and when it ends, what a proxy in front needs, and what a sign-out does not do;
the research docs record Oleksii's three choices and the shapes.

`crates/data-avalanche-config/src/field_docs.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/field_docs.rs b/crates/data-avalanche-config/src/field_docs.rs
index 2db41e4..34ecd77 100644
--- a/crates/data-avalanche-config/src/field_docs.rs
+++ b/crates/data-avalanche-config/src/field_docs.rs
@@ -336,7 +336,7 @@ pub const FIELD_DOCS: &[FieldDoc] = &[
     f(
         "globals.console.session_ttl",
         Value("8h"),
-        "How long a session lasts without activity, from 5 minutes to 7 days (V-132).",
+        "How long a session lasts after sign-in, however it is used, from 5 minutes to 7 days (V-132). With `oauth2`, the provider's token is refreshed within it when the provider issues refresh tokens.",
     ),
     f(
         "globals.console.username_claim",
````

`doc/research/API_RBAC_and_Versioning.md`:

````diff
diff --git a/doc/research/API_RBAC_and_Versioning.md b/doc/research/API_RBAC_and_Versioning.md
index f740405..3925bb3 100644
--- a/doc/research/API_RBAC_and_Versioning.md
+++ b/doc/research/API_RBAC_and_Versioning.md
@@ -1,5 +1,5 @@
 ---
-updated: 2026-11-06
+updated: 2026-11-24
 domain: Auth
 tags: [rbac, api, authorization, roles, jwt, oauth2, versioning, management-api, cdc]
 resolves: [G-16, G-9.1]
@@ -130,6 +130,15 @@ As built in M9a (D-M9a.6, and the review fixes in the plan's deviations):
 - A node that left the cluster keeps the auth of its last generation until it is stopped
   (DA-API-14).
 
+As built in M14 (Admin_Console §9.1): with the console on and `auth` `basic` or `oauth2`, a
+request without `Authorization` may carry the console's session cookie instead. Its person is
+verified again on every request (the basic user and password; the provider's access token, as a
+bearer token is), so the role and the recorded name come from the same verification. A request the
+session authenticates that changes something sends the session's anti-forgery token
+(`X-CSRF-Token`); without it, 403, not recorded as a role refusal. A token whose roles map to no
+role is refused naming the roles it carried. The OpenAPI document lists the cookie as the
+`session` scheme.
+
 ---
 
 ## §4 — IdP role mapping (D-16.3 — configurable)
````

`doc/research/Admin_Console.md`:

````diff
diff --git a/doc/research/Admin_Console.md b/doc/research/Admin_Console.md
index e1281a8..2eecfe3 100644
--- a/doc/research/Admin_Console.md
+++ b/doc/research/Admin_Console.md
@@ -1,5 +1,5 @@
 ---
-updated: 2026-11-19
+updated: 2026-11-24
 domain: Architecture
 tags: [admin-console, web-ui, management-api, auth, sessions, audit, metrics-history, prometheus, v1.1]
 ---
@@ -263,6 +263,59 @@ One sign-in page; what follows depends on `globals.api.auth`:
 - **Plain HTTP:** `Secure` cookies work on `localhost`; elsewhere TLS is expected (on the listener
   or a proxy in front, as V-052 already assumes).
 
+### 9.1 Sessions as built (M14)
+
+Oleksii decided three things on 2026-11-24: a session lasts `session_ttl` **from sign-in**, however
+it is used (the config spec says so since S-M14.1); the sign-in and error pages are **rendered by
+the server** (M15's app links to them); and failed basic sign-ins are counted **per client
+address**. The rest is Claude's call.
+
+- **The cookie.** `da_session`, AES-256-GCM under a key derived from `session_key`, the key's id
+  beside the ciphertext so a changed key is told from an altered cookie; `HttpOnly`, `Secure`,
+  `SameSite=Strict`, `Path=/`. One too long for a cookie (a provider's long tokens) goes on in
+  `da_session.1` … `.3`; one longer starts no session. It holds when the person signed in, the
+  anti-forgery token, and what verifies them: the basic user and a keyed fingerprint of the
+  password, or the provider's access token and its refresh token. Nothing is kept server-side.
+- **Every request verifies it again**, as the API verifies credentials: a basic session's user and
+  password must still be the configured ones; an `oauth2` session's access token verifies as a
+  bearer token does (issuer, audience, signature, roles). It ends `session_ttl` after sign-in, or
+  sooner when that fails, when `session_key` or `auth` changes, or when the console is switched
+  off. A request with `Authorization` is taken by its credentials, never its cookie.
+- **The API takes it too**, so the console's app calls `/v1` with no token in page JavaScript. A
+  request the session authenticates that changes something sends the session's anti-forgery token
+  (`X-CSRF-Token`, from `GET /ui/session`): `403` without it, before its role is looked at.
+- **Peers open it** (M13f's carry-in): a node passes a caller's session cookies and anti-forgery
+  token on with its `Authorization` (node actions, another node's detail, a stream's follows), and
+  the peer verifies them with the key every node has.
+- **A stream ends with it** (M13f's carry-in): an open stream checks its caller every minute and a
+  second after the credentials' expiry; once they no longer verify it is sent `reset`
+  (`credentials`) and ends, and the page opens it again.
+- **`oauth2`.** Authorization code with PKCE (`S256`) and a new state, kept in a sealed
+  `da_oauth2` cookie (`SameSite=Lax`, the callback's path, 10 min); the code exchanged with the
+  console's client (`client_secret_basic`, or `client_secret_post` when that is all the provider
+  takes); the token verified before a session starts, so a token whose roles map to nothing is
+  "Forbidden" at sign-in. The callback answers a page that goes on to `next` from the console's own
+  origin, since a `Strict` cookie doesn't follow a redirect the provider began. A session whose
+  token expires within a minute is refreshed by the next page asked for, or by
+  `POST /ui/session/refresh` (the app's, with the anti-forgery token); one node makes one refresh
+  for a refresh token at a time and gives its outcome to the same token for 30 s, since a provider
+  that rotates refresh tokens refuses one used twice. The ID token is kept in `da_id`, sent only to
+  the sign-out, for the provider's `id_token_hint`.
+- **The routes under `/ui/`:** `GET /ui/sign-in` (the form, or the way to the provider),
+  `POST /ui/sign-in` (basic), `GET /ui/oauth2/start`, `GET /ui/oauth2/callback`,
+  `POST /ui/sign-out`, `GET /ui/session`, `POST /ui/session/refresh`, `/ui/assets/…`, and every
+  other page (M15's app; until then, who is signed in). A page asked for with no credentials goes
+  to the sign-in; with credentials that no longer verify it is the 401 page, saying why.
+- **Hardening.** A Content-Security-Policy that runs no inline script or style
+  (`script-src 'self'`, `form-action 'self'` and the provider's origins, `frame-ancestors
+  'none'`), `X-Frame-Options: DENY`, `nosniff`, `Referrer-Policy: no-referrer`, `no-store`. A form
+  posted to `/ui/` from another site (`Sec-Fetch-Site`, else `Origin` against `Host`) is refused.
+  `next` is always a path under `/ui/`. Five failed basic sign-ins in a row from one address make
+  it wait a minute on that node; behind a proxy every client shares the proxy's address.
+- **Signing out** removes the cookies; with `oauth2` it goes on to the provider's end-session
+  endpoint, when it has one. Since nothing is kept server-side, a copy of the cookie taken before
+  works until the session ends: a short `session_ttl`, or a new `session_key`, bounds that.
+
 ## 10. Front end and serving
 
 All Claude's calls, "follow best practices" (Oleksii):
````

`docs/src/content/docs/deployment/console.md`:

````diff
diff --git a/docs/src/content/docs/deployment/console.md b/docs/src/content/docs/deployment/console.md
new file mode 100644
index 0000000..35e37eb
--- /dev/null
+++ b/docs/src/content/docs/deployment/console.md
@@ -0,0 +1,77 @@
+---
+title: Web console sign-in
+description: Switching the web console on, how people sign in under each API auth mode, and what a session is.
+sidebar:
+  order: 5
+---
+
+The web console is served by every node, on the API's listener, under `/ui/`. It is off until
+`globals.console.enabled: true`, and a reload turns it on or off. People sign in as the API's
+auth mode (`globals.api.auth`) says, and get the same role there as through the API.
+
+## Switching it on
+
+```yaml
+globals:
+  console:
+    enabled: true
+    session_key: ${CONSOLE_SESSION_KEY}    # basic and oauth2
+    public_url: https://cdc.example.com    # oauth2
+```
+
+`session_key` encrypts the console's session cookie. Give every node the same value, at least 32
+bytes, from the environment or a secret; it is never kept in object storage. Changing it signs
+everyone out. Make one with `openssl rand -base64 48`.
+
+Serve the console over HTTPS, on the listener (`globals.api.tls`) or on a proxy in front of it: its
+cookies are `Secure`, which browsers send over plain HTTP only to `localhost`.
+
+## Signing in
+
+| `globals.api.auth` | What a person sees |
+|---|---|
+| `none` | No sign-in: everyone is `admin`, and every page says the cluster has no authentication. |
+| `basic` | A sign-in form for the configured user. Five wrong passwords in a row from one address make it wait a minute. |
+| `oauth2` | "Continue with" the provider (`oauth2.idp_name`, else the issuer's host); the page goes there by itself after 5 s, unless the person stays, or just signed out, or the sign-in failed. |
+| `jwt` | No sign-in and no session: a proxy in front of the console sends a bearer token with every request. Without a valid one, the page says "No valid token" and why. |
+
+A person whose roles map to no role on the cluster sees "Forbidden", with the roles their token
+carried; with `oauth2` that happens at sign-in.
+
+### With `oauth2`
+
+Register the console as a client at the provider:
+
+- `client_id` (`globals.api.oauth2.client_id`), and `client_secret` only for a confidential client;
+- the redirect URI `<public_url>/ui/oauth2/callback`;
+- the sign-out's return address `<public_url>/ui/sign-in?signed_out`, where the provider allows one.
+
+The console signs in with the authorization code flow and PKCE, and exchanges the code itself. The
+provider's endpoints come from its discovery document (`discovery_url`, else the issuer's
+`/.well-known/openid-configuration`), so the node must reach the provider. The access token it
+gets is verified as any API token is: its issuer, audience and roles.
+
+When the provider issues refresh tokens, a session's token is refreshed shortly before it
+expires. Signing out ends the session and signs out at the provider, when it has an end-session
+endpoint.
+
+## Sessions
+
+With `basic` and `oauth2`, signing in starts a session: an encrypted cookie that every node reads,
+so a load balancer may send each request anywhere. It ends `session_ttl` after sign-in (8 h by
+default), however it is used, and sooner when:
+
+- the basic password changes;
+- the provider's token expires and can't be refreshed, or its roles no longer map;
+- `session_key` or `globals.api.auth` changes.
+
+An ended session's page says why, with a way to sign in again and come back.
+
+Signing out removes the browser's cookie. The cluster keeps no list of sessions, so a copy of the
+cookie taken before the sign-out still works until the session ends: keep `session_ttl` short where
+that matters, or change `session_key` to end every session at once.
+
+The API takes the session as it takes credentials, when a request has no `Authorization` header.
+A request that changes something sends the session's anti-forgery token in `X-CSRF-Token`
+(`GET /ui/session` gives it); without it the answer is `403`. A live stream (`GET /v1/stream`)
+ends with its session, with a `reset` message, and the page opens it again.
````

Then regenerate `docs/src/content/docs/reference/configuration.md`: `cargo xtask docs` writes
`session_ttl`'s row from `field_docs.rs`.

- [ ] **Step 2: Build the site**

Run: `cd docs && npm ci && npm run build`
Expected: the build ends with "All internal links are valid" and 32 pages.

- [ ] **Step 3: Run the gates**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 4: Commit**

```bash
git add crates/data-avalanche-config/src/field_docs.rs doc/research/API_RBAC_and_Versioning.md doc/research/Admin_Console.md docs/src/content/docs/deployment/console.md docs/src/content/docs/reference/configuration.md
git commit -m "docs: the console's sign-in and sessions; Admin_Console §9.1; API_RBAC §3"
```

---

### Task 6: The scenario on three nodes (`data-avalanche` E2E)

**Files:**
- Create: `crates/data-avalanche/tests/e2e/console_sessions.rs`
- Modify: `crates/data-avalanche/tests/e2e/harness.rs`
- Modify: `crates/data-avalanche/tests/e2e/idp.rs`
- Modify: `crates/data-avalanche/tests/e2e/main.rs`

**Interfaces:**
- Consumes: the E2E harness's `Cluster` (M8) and its identity provider (`idp.rs`, M11b); Tasks
  1–4 through the binary.
- Produces: `Proc::exchange(&self, method, path, headers, body) -> Option<(u16, String,
  String)>` (status, head, body), `Proc::stream_with(&self, path, headers)`, `Proc::port()`
  (harness); `idp::Provider::{start(roles), api(), approve(location) -> String}`,
  `query_value(url, name)`; `console_sessions::{a_basic_session_started_on_one_node_is_every_node_s,
  an_oauth2_session_started_through_the_provider_is_every_node_s}`.

- [ ] **Step 1: Write the scenario**

For each mode, on three nodes: a page asked for without credentials goes to the sign-in; the
sign-in on `n1` (a wrong password first, with `basic`; through the provider, with `oauth2`);
then on every node the landing page and `GET /ui/session` with one anti-forgery token; a pause on
`n3` refused without it and taken with it, recorded under the session's name; `n3`'s detail
through `n1`; a stream on `n1` that follows `n2` and `n3` with the session; the sign-out on `n2`
(on to the provider with `oauth2`).

`crates/data-avalanche/tests/e2e/console_sessions.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/console_sessions.rs b/crates/data-avalanche/tests/e2e/console_sessions.rs
new file mode 100644
index 0000000..3b9fe97
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/console_sessions.rs
@@ -0,0 +1,285 @@
+//! The console's sign-in and sessions (Admin_Console §9; M14) on three nodes: a session started
+//! on one node is every node's, for its pages, the API, a node's detail asked through another,
+//! and a live stream that follows the other nodes with it; a request that changes something sends
+//! the session's anti-forgery token, or is refused. With `basic` (a form, its attempts counted)
+//! and `oauth2` (a provider's sign-in, the sign-out taken on to it).
+
+use std::io::Read;
+use std::time::Duration;
+
+use base64::Engine;
+use base64::engine::general_purpose::URL_SAFE_NO_PAD;
+use data_avalanche_testkit::require_it;
+use serde_json::{Value, json};
+
+use crate::harness::{Doc, Options, eventually};
+use crate::idp::{Provider, query_value};
+use crate::nodes::{Cluster, ITEMS};
+
+/// A session key made at run time: 48 random bytes.
+fn session_key() -> String {
+    let mut b = [0u8; 48];
+    std::fs::File::open("/dev/urandom")
+        .and_then(|mut f| f.read_exact(&mut b))
+        .expect("the system's random source");
+    URL_SAFE_NO_PAD.encode(b)
+}
+
+/// `n1`, `n2` and `n3` running the test's pipeline, its rows landed, with `globals.api` entries
+/// `api` and the console on, `console` adding to it.
+async fn cluster(test: &str, api: &str, console: &str) -> Cluster {
+    let globals = format!(
+        r#"console: {{ enabled: true, session_key: "{}"{console} }}"#,
+        session_key()
+    );
+    let mut c = Cluster::with(
+        test,
+        ITEMS,
+        "",
+        Options {
+            tables: ", snapshots: { mode: never }",
+            cluster: ", rebalance_stabilization: 3s",
+            api,
+            globals: &globals,
+            ..Options::default()
+        },
+    )
+    .await;
+    c.start_all().await;
+    c.e.insert_series("items", 5, Doc::Row).await;
+    c.landed().await;
+    c
+}
+
+/// The values of header `name` in `head`.
+fn headers_of<'h>(head: &'h str, name: &str) -> Vec<&'h str> {
+    head.lines()
+        .filter_map(|l| l.split_once(": "))
+        .filter(|(n, _)| n.eq_ignore_ascii_case(name))
+        .map(|(_, v)| v)
+        .collect()
+}
+
+/// The `Cookie` a browser sends back after `head`'s `Set-Cookie`s.
+fn kept(head: &str) -> String {
+    headers_of(head, "set-cookie")
+        .into_iter()
+        .filter(|c| !c.contains("Max-Age=0"))
+        .map(|c| c.split(';').next().unwrap())
+        .collect::<Vec<_>>()
+        .join("; ")
+}
+
+/// Every node takes the session `cookie` as `name`, with `role`: its pages and session, a
+/// pause only with its anti-forgery token (recorded under its name), a node's detail through
+/// another node, and a stream that follows the other two with it.
+async fn every_node_takes(c: &Cluster, cookie: &str, name: &str, role: &str) {
+    let test = c.e.test.clone();
+    let with = [("Cookie", cookie)];
+    let mut csrf = None;
+    for n in ["n1", "n2", "n3"] {
+        let p = c.proc(n);
+        let (code, _, page) = p.exchange("GET", "/ui/", &with, "").await.unwrap();
+        assert_eq!(code, 200, "{n}: {page}");
+        assert!(
+            page.contains(&format!(
+                "You are <b>{name}</b>, signed in as <b>{role}</b>."
+            )),
+            "{n}: {page}"
+        );
+        let (code, _, text) = p.exchange("GET", "/ui/session", &with, "").await.unwrap();
+        assert_eq!(code, 200, "{n}: {text}");
+        let v: Value = serde_json::from_str(&text).unwrap();
+        assert_eq!(
+            (&v["name"], &v["role"]),
+            (&json!(name), &json!(role)),
+            "{n}"
+        );
+        let token = v["csrf"].as_str().unwrap().to_owned();
+        assert_eq!(
+            csrf.get_or_insert(token.clone()),
+            &token,
+            "{n}: one session"
+        );
+    }
+    let csrf = csrf.unwrap();
+
+    // A pause on n3: refused without the anti-forgery token, taken with it, recorded under the
+    // session's name.
+    let pause = format!("/v1/pipelines/{test}/pause");
+    let n3 = c.proc("n3");
+    let (code, _, text) = n3.exchange("POST", &pause, &with, "").await.unwrap();
+    assert_eq!(code, 403, "{text}");
+    assert!(text.contains("anti-forgery token"), "{text}");
+    let both = [("Cookie", cookie), ("X-CSRF-Token", csrf.as_str())];
+    let (code, _, text) = n3.exchange("POST", &pause, &both, "").await.unwrap();
+    assert_eq!(code, 202, "{text}");
+    let query = format!("/v1/events?kind=action.pipeline.pause&object={test}");
+    let n2 = c.proc("n2");
+    eventually("the pause's event", Duration::from_secs(30), async || {
+        let Some((200, _, text)) = n2.exchange("GET", &query, &with, "").await else {
+            return false;
+        };
+        let v: Value = serde_json::from_str(&text).unwrap();
+        v["events"]
+            .as_array()
+            .is_some_and(|e| e.iter().any(|e| e["actor"]["name"] == name))
+    })
+    .await;
+    let resume = format!("/v1/pipelines/{test}/resume");
+    assert_eq!(
+        n3.exchange("POST", &resume, &both, "").await.unwrap().0,
+        202
+    );
+
+    // Another node's detail, through n1: n3 opens the session itself.
+    let n1 = c.proc("n1");
+    let (code, _, text) = n1.exchange("GET", "/v1/nodes/n3", &with, "").await.unwrap();
+    assert_eq!(code, 200, "{text}");
+
+    // A stream on n1 follows n2 and n3 with the session.
+    let mut s = n1
+        .stream_with("/v1/stream?kind=node", &with)
+        .await
+        .expect("the stream opens");
+    let (kind, hello) = s.next().await.expect("hello");
+    assert_eq!(
+        (kind.as_str(), &hello["follows"]),
+        ("hello", &json!(["n2", "n3"]))
+    );
+    let followed = tokio::time::timeout(Duration::from_secs(30), async {
+        let mut followed = std::collections::BTreeSet::new();
+        while followed.len() < 2 {
+            let (kind, v) = s.next().await.expect("the stream goes on");
+            if kind == "peer" {
+                assert_eq!(v["state"], "following", "{v}");
+                followed.insert(v["peer"].as_str().unwrap().to_owned());
+            }
+        }
+        followed
+    })
+    .await
+    .expect("both peers followed within 30 s");
+    assert_eq!(followed, ["n2".to_owned(), "n3".to_owned()].into());
+}
+
+#[tokio::test]
+async fn a_basic_session_started_on_one_node_is_every_node_s() {
+    require_it!();
+    let c = cluster(
+        "console_basic",
+        ", auth: basic, user: ops, password: s3cret",
+        "",
+    )
+    .await;
+    let n1 = c.proc("n1");
+    let (code, head, _) = n1.exchange("GET", "/ui/", &[], "").await.unwrap();
+    assert_eq!(
+        (code, headers_of(&head, "location")),
+        (303, vec!["/ui/sign-in?next=%2Fui%2F"])
+    );
+    let form = [("Content-Type", "application/x-www-form-urlencoded")];
+    let wrong = "user=ops&password=nope&next=%2Fui%2F";
+    let (code, _, page) = n1
+        .exchange("POST", "/ui/sign-in", &form, wrong)
+        .await
+        .unwrap();
+    assert_eq!(code, 401);
+    assert!(
+        page.contains("4 attempts left before a 1 min wait."),
+        "{page}"
+    );
+    let right = "user=ops&password=s3cret&next=%2Fui%2F";
+    let (code, head, _) = n1
+        .exchange("POST", "/ui/sign-in", &form, right)
+        .await
+        .unwrap();
+    assert_eq!((code, headers_of(&head, "location")), (303, vec!["/ui/"]));
+    let cookie = kept(&head);
+    every_node_takes(&c, &cookie, "ops", "admin").await;
+
+    // Signed out on n2: the session's cookies removed.
+    let out = [
+        ("Cookie", cookie.as_str()),
+        ("Sec-Fetch-Site", "same-origin"),
+    ];
+    let (code, head, _) = c
+        .proc("n2")
+        .exchange("POST", "/ui/sign-out", &out, "")
+        .await
+        .unwrap();
+    assert_eq!(
+        (code, headers_of(&head, "location")),
+        (303, vec!["/ui/sign-in?signed_out"])
+    );
+    assert!(
+        headers_of(&head, "set-cookie")
+            .iter()
+            .any(|c| c.starts_with("da_session=;") && c.contains("Max-Age=0"))
+    );
+    c.stop().await;
+}
+
+#[tokio::test]
+async fn an_oauth2_session_started_through_the_provider_is_every_node_s() {
+    require_it!();
+    let provider = Provider::start(&["operator"]).await;
+    let c = cluster(
+        "console_oauth2",
+        &provider.api(),
+        r#", public_url: "http://127.0.0.1:${E2E_API_PORT}""#,
+    )
+    .await;
+    let n1 = c.proc("n1");
+    let (code, head, page) = n1.exchange("GET", "/ui/sign-in", &[], "").await.unwrap();
+    assert_eq!(code, 200);
+    let host = provider
+        .issuer
+        .trim_start_matches("http://")
+        .split(':')
+        .next()
+        .unwrap();
+    assert!(page.contains(&format!("Continue with {host}")), "{page}");
+    let (code, head2, _) = n1
+        .exchange("GET", "/ui/oauth2/start?next=%2Fui%2F", &[], "")
+        .await
+        .unwrap();
+    assert_eq!(code, 303, "{head}");
+    let back = provider.approve(headers_of(&head2, "location")[0]);
+    let path = back
+        .strip_prefix(&format!("http://127.0.0.1:{}", n1.port()))
+        .expect("back at n1, its public_url")
+        .to_owned();
+    let left_with = kept(&head2);
+    let (code, head, page) = n1
+        .exchange("GET", &path, &[("Cookie", left_with.as_str())], "")
+        .await
+        .unwrap();
+    assert_eq!(code, 200, "{page}");
+    let cookie = kept(&head);
+    every_node_takes(&c, &cookie, "alice.smith", "operator").await;
+
+    // Signed out on n2: on to the provider, with the ID token, and back to n2's sign-in.
+    let n2 = c.proc("n2");
+    let out = [
+        ("Cookie", cookie.as_str()),
+        ("Sec-Fetch-Site", "same-origin"),
+    ];
+    let (code, head, _) = n2.exchange("POST", "/ui/sign-out", &out, "").await.unwrap();
+    assert_eq!(code, 303);
+    let to = headers_of(&head, "location")[0];
+    assert!(
+        to.starts_with(&format!("{}/end-session?", provider.issuer)),
+        "{to}"
+    );
+    assert_eq!(query_value(to, "client_id").as_deref(), Some("cdc-console"));
+    assert_eq!(
+        query_value(to, "post_logout_redirect_uri"),
+        Some(format!(
+            "http://127.0.0.1:{}/ui/sign-in?signed_out",
+            n2.port()
+        ))
+    );
+    assert!(query_value(to, "id_token_hint").is_some(), "{to}");
+    c.stop().await;
+}
````

`crates/data-avalanche/tests/e2e/harness.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
index e06c823..c5e3007 100644
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -1042,11 +1042,22 @@ impl Proc {
     /// answer's body is the stream's bytes as sent. `None` when nothing answers or the answer
     /// isn't `200`.
     pub async fn stream(&self, path: &str) -> Option<Sse> {
-        let auth = self
-            .token
+        let auth = self.token.as_deref().map(|t| format!("Bearer {t}"));
+        let headers: Vec<(&str, &str)> = auth
             .as_deref()
-            .map_or(String::new(), |t| format!("Authorization: Bearer {t}\r\n"));
-        let request = format!("GET {path} HTTP/1.0\r\nHost: t\r\n{auth}\r\n");
+            .map(|a| ("Authorization", a))
+            .into_iter()
+            .collect();
+        self.stream_with(path, &headers).await
+    }
+
+    /// [`Self::stream`], with `headers` (a session's cookie) instead of the token.
+    pub async fn stream_with(&self, path: &str, headers: &[(&str, &str)]) -> Option<Sse> {
+        let headers: String = headers
+            .iter()
+            .map(|(k, v)| format!("{k}: {v}\r\n"))
+            .collect();
+        let request = format!("GET {path} HTTP/1.0\r\nHost: t\r\n{headers}\r\n");
         let tcp = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
             .await
             .ok()?;
@@ -1082,6 +1093,11 @@ impl Proc {
         })
     }
 
+    /// The API's port.
+    pub fn port(&self) -> u16 {
+        self.port
+    }
+
     pub fn log(&self) -> String {
         std::fs::read_to_string(&self.log).unwrap_or_default()
     }
@@ -1106,9 +1122,29 @@ impl Proc {
         path: &str,
         body: &str,
     ) -> Option<(u16, String, String)> {
-        let auth = token.map_or(String::new(), |t| format!("Authorization: Bearer {t}\r\n"));
+        let auth = token.map(|t| format!("Bearer {t}"));
+        let mut headers = vec![("Content-Type", "application/yaml")];
+        if let Some(a) = &auth {
+            headers.push(("Authorization", a));
+        }
+        self.exchange(method, path, &headers, body).await
+    }
+
+    /// A request with `headers` and `body`: the status code, the head and the body. `None` when
+    /// nothing answers.
+    pub async fn exchange(
+        &self,
+        method: &str,
+        path: &str,
+        headers: &[(&str, &str)],
+        body: &str,
+    ) -> Option<(u16, String, String)> {
+        let headers: String = headers
+            .iter()
+            .map(|(k, v)| format!("{k}: {v}\r\n"))
+            .collect();
         let request = format!(
-            "{method} {path} HTTP/1.1\r\nHost: t\r\n{auth}Content-Type: application/yaml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
+            "{method} {path} HTTP/1.1\r\nHost: t\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
             body.len()
         );
         let tcp = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
````

`crates/data-avalanche/tests/e2e/idp.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/idp.rs b/crates/data-avalanche/tests/e2e/idp.rs
index a8862a3..03f7954 100644
--- a/crates/data-avalanche/tests/e2e/idp.rs
+++ b/crates/data-avalanche/tests/e2e/idp.rs
@@ -91,6 +91,226 @@ impl Drop for Idp {
     }
 }
 
+/// An OpenID provider for the console's `oauth2` sign-in, on a loopback port: its discovery
+/// document, its JWKS, a token endpoint that exchanges any code it issued and refreshes, and an
+/// end-session endpoint. Its authorization step approves at once, in the test's own process
+/// ([`Provider::approve`]). Its tokens name `alice` (`preferred_username` `alice.smith`) with
+/// `roles`.
+pub struct Provider {
+    pub issuer: String,
+    served: JoinHandle<()>,
+    codes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
+}
+
+/// `s`, percent-decoded (a query's value).
+fn decoded(s: &str) -> String {
+    let b = s.as_bytes();
+    let mut out = Vec::with_capacity(b.len());
+    let mut i = 0;
+    while i < b.len() {
+        match b[i] {
+            b'%' if i + 2 < b.len() => {
+                let hex = std::str::from_utf8(&b[i + 1..i + 3]).unwrap();
+                out.push(u8::from_str_radix(hex, 16).unwrap());
+                i += 3;
+            }
+            b'+' => {
+                out.push(b' ');
+                i += 1;
+            }
+            c => {
+                out.push(c);
+                i += 1;
+            }
+        }
+    }
+    String::from_utf8(out).unwrap()
+}
+
+/// The value of `name` in `url`'s query.
+pub fn query_value(url: &str, name: &str) -> Option<String> {
+    let query = url.split_once('?')?.1;
+    query
+        .split('&')
+        .filter_map(|p| p.split_once('='))
+        .find(|(k, _)| *k == name)
+        .map(|(_, v)| decoded(v))
+}
+
+/// Reads one HTTP request from `s`: its request line and its body.
+async fn read_request(s: &mut tokio::net::TcpStream) -> Option<(String, String)> {
+    let mut buf = Vec::new();
+    let mut chunk = [0u8; 4096];
+    let head_end = loop {
+        let n = s.read(&mut chunk).await.ok()?;
+        if n == 0 {
+            return None;
+        }
+        buf.extend_from_slice(&chunk[..n]);
+        if let Some(at) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
+            break at + 4;
+        }
+    };
+    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
+    let length: usize = head
+        .lines()
+        .find_map(|l| {
+            let (k, v) = l.split_once(':')?;
+            k.eq_ignore_ascii_case("content-length")
+                .then(|| v.trim().parse().ok())?
+        })
+        .unwrap_or(0);
+    while buf.len() < head_end + length {
+        let n = s.read(&mut chunk).await.ok()?;
+        if n == 0 {
+            break;
+        }
+        buf.extend_from_slice(&chunk[..n]);
+    }
+    let line = head.lines().next()?.to_owned();
+    let body = String::from_utf8_lossy(&buf[head_end..]).into_owned();
+    Some((line, body))
+}
+
+impl Provider {
+    /// Serves on a loopback port; its tokens carry `roles`.
+    pub async fn start(roles: &[&str]) -> Self {
+        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
+        let issuer = format!("http://{}", listener.local_addr().unwrap());
+        let key = std::sync::Arc::new(KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap());
+        let raw = key.public_key_raw();
+        let jwks = json!({ "keys": [{
+            "kty": "EC", "crv": "P-256", "use": "sig", "alg": "ES256", "kid": "k1",
+            "x": URL_SAFE_NO_PAD.encode(&raw[1..33]),
+            "y": URL_SAFE_NO_PAD.encode(&raw[33..65]),
+        }] })
+        .to_string();
+        let discovery = json!({
+            "issuer": issuer,
+            "jwks_uri": format!("{issuer}/jwks"),
+            "authorization_endpoint": format!("{issuer}/authorize"),
+            "token_endpoint": format!("{issuer}/token"),
+            "end_session_endpoint": format!("{issuer}/end-session"),
+        })
+        .to_string();
+        let roles: Vec<String> = roles.iter().map(|r| (*r).to_owned()).collect();
+        let at = issuer.clone();
+        let served = tokio::spawn(async move {
+            let refreshes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
+            loop {
+                let Ok((mut s, _)) = listener.accept().await else {
+                    return;
+                };
+                let (jwks, discovery, key, roles, issuer, refreshes) = (
+                    jwks.clone(),
+                    discovery.clone(),
+                    std::sync::Arc::clone(&key),
+                    roles.clone(),
+                    at.clone(),
+                    std::sync::Arc::clone(&refreshes),
+                );
+                tokio::spawn(async move {
+                    let Some((line, body)) = read_request(&mut s).await else {
+                        return;
+                    };
+                    let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
+                    let (code, answer) = if path.starts_with("/.well-known/openid-configuration") {
+                        (200, discovery)
+                    } else if path.starts_with("/jwks") {
+                        (200, jwks)
+                    } else if path.starts_with("/token") {
+                        let n = refreshes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
+                        let now = jsonwebtoken::get_current_timestamp();
+                        let sign = |claims: serde_json::Value| {
+                            let mut header = Header::new(Algorithm::ES256);
+                            header.kid = Some("k1".into());
+                            jsonwebtoken::encode(
+                                &header,
+                                &claims,
+                                &EncodingKey::from_ec_der(&key.serialize_der()),
+                            )
+                            .unwrap()
+                        };
+                        let access = sign(json!({
+                            "iss": issuer, "aud": "cdc", "sub": "alice",
+                            "preferred_username": "alice.smith", "exp": now + 600, "roles": roles,
+                        }));
+                        let id = sign(json!({
+                            "iss": issuer, "aud": "cdc-console", "sub": "alice", "exp": now + 600,
+                        }));
+                        let granted = body.contains("grant_type=authorization_code")
+                            || body.contains("grant_type=refresh_token");
+                        if granted {
+                            (
+                                200,
+                                json!({
+                                    "access_token": access, "token_type": "Bearer",
+                                    "expires_in": 600, "refresh_token": format!("refresh-{n}"),
+                                    "id_token": id,
+                                })
+                                .to_string(),
+                            )
+                        } else {
+                            (
+                                400,
+                                json!({ "error": "unsupported_grant_type" }).to_string(),
+                            )
+                        }
+                    } else if path.starts_with("/end-session") {
+                        (200, "{}".to_owned())
+                    } else {
+                        (404, "{}".to_owned())
+                    };
+                    let reason = if code == 200 { "OK" } else { "Error" };
+                    let answer = format!(
+                        "HTTP/1.1 {code} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
+                        answer.len()
+                    );
+                    let _ = s.write_all(answer.as_bytes()).await;
+                    let _ = s.shutdown().await;
+                });
+            }
+        });
+        Self {
+            issuer,
+            served,
+            codes: std::sync::Arc::default(),
+        }
+    }
+
+    /// `globals.api` entries: `auth: oauth2` for this provider, found through its discovery
+    /// document, audience `cdc`, the roles in `roles`, and the console's client `cdc-console`.
+    pub fn api(&self) -> String {
+        format!(
+            r#", auth: oauth2, oauth2: {{ issuer: "{i}", audience: cdc, discovery_url: "{i}/.well-known/openid-configuration", roles_claim: roles, client_id: cdc-console }}"#,
+            i = self.issuer
+        )
+    }
+
+    /// The provider's approval of the authorization request at `location`: where it sends the
+    /// browser back, with a code and the request's state.
+    pub fn approve(&self, location: &str) -> String {
+        assert!(
+            location.starts_with(&format!("{}/authorize?", self.issuer)),
+            "{location}"
+        );
+        assert_eq!(
+            query_value(location, "code_challenge_method").as_deref(),
+            Some("S256")
+        );
+        let back = query_value(location, "redirect_uri").expect("a redirect URI");
+        let state = query_value(location, "state").expect("a state");
+        let n = self.codes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
+        format!("{back}?code=code-{n}&state={state}")
+    }
+}
+
+impl Drop for Provider {
+    fn drop(&mut self) {
+        self.served.abort();
+    }
+}
+
 /// A certificate for `127.0.0.1` and its key, written to `dir`: the `globals.api` entry that
 /// serves them, and the certificate to trust.
 pub fn tls_files(dir: &Path) -> (String, CertificateDer<'static>) {
````

`crates/data-avalanche/tests/e2e/main.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index 8a11e5b..0509668 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -11,6 +11,7 @@ mod cluster;
 mod console_history;
 mod console_nodes;
 mod console_pipelines;
+mod console_sessions;
 mod console_stream;
 mod crashes;
 mod events;
````

- [ ] **Step 2: Run it**

Run, with the emulators up (`test-infra/up.sh`):

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e console_sessions -- --test-threads=1
for m in mysql-84 mysql-84-nogtid mariadb-118; do
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_E2E_SOURCE=mysql DATA_AVALANCHE_MYSQL=$m \
    cargo test -p data-avalanche --features e2e-hooks --test e2e console_sessions -- --test-threads=1
done
```

Expected: `2 passed` on each, in about 35 s. The scenario passes at once: Tasks 1–4 built what it
checks.

- [ ] **Step 3: Run the gates**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 4: Commit**

```bash
git add crates/data-avalanche/tests/e2e/console_sessions.rs crates/data-avalanche/tests/e2e/harness.rs crates/data-avalanche/tests/e2e/idp.rs crates/data-avalanche/tests/e2e/main.rs
git commit -m "test(e2e): console sessions on three nodes, basic and oauth2"
```

---

## Exit

- Every task committed, one commit each, after the plan's and the spec patch's commits; the gates
  pass on the last one (`cargo fmt --all --check`, both clippy runs, `cargo test --workspace`).
- With the emulators up: the whole E2E suite on PostgreSQL 18 (`DATA_AVALANCHE_IT=1
  DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e --
  --test-threads=1`), and Task 6's scenario on `mysql-84`, `mysql-84-nogtid` and `mariadb-118`.
- `cargo xtask docs --check` and `cd docs && npm run build` (its link check) pass.
- At the plan's verification (the code above, on `d18dbae`, each task's red and green commits
  apart): every Step 2 failure as stated; each task's gates and its tests pass at its commit;
  at Task 6, `cargo test --workspace` 1822 passed, both clippy runs and `cargo fmt --all --check`
  clean, `cargo xtask docs --check` held; Task 6's scenario passed on PostgreSQL 18 (34 s),
  `mysql-84` (34 s), `mysql-84-nogtid` (34 s) and `mariadb-118` (34 s); the whole E2E suite passed
  on PostgreSQL 18 (59 tests, 859 s); the site built (32 pages, link check clean). The E2E runs
  were on the tree before Task 4 re-exported `SIGN_IN_COOKIE` and `ID_COOKIE` at the crate root,
  a one-line change the gates ran after.
- Then, on Oleksii's request: the whole-branch review and its fixes, the push, the PR.

## Deviations during execution

Each task was reviewed after its implementation; where a review found a problem in the plan's
own code, the task's commit was amended in a fix round. The rulings below are Oleksii's where
marked, the executor's otherwise.

**Task 1** (`5db8733`):
- While the console's sessions are on, a `401` for a request without `Authorization` carries
  `WWW-Authenticate: Session realm="data-avalanche"`, not `Basic`, so a browser shows no
  password popup; with `Authorization`, `Basic` or `Bearer` as before; with the console off,
  unchanged (Oleksii).
- The session cookie is `__Host-da_session` (chunks `.1`–`.3`), not `da_session`; `da_oauth2`
  and `da_id` keep their names, since they are scoped to a path and `__Host-` needs `Path=/`
  (Oleksii).
- A session under `Mode::Refused` (inconsistent settings) is answered `503`, as a header is and
  as M13f's stream is.
- New tests: `a_refused_session_or_no_credentials_are_challenged_for_a_session`,
  `a_session_under_inconsistent_settings_is_answered_unavailable`, and a `viewer` session whose
  mutating call carries the right anti-forgery token, refused `403` for its role.

**Task 2** (`4aceffa`): as planned.

**Task 3** (`8aa9d8a`, two fix rounds):
- The attempts table is bounded: it is pruned at most once a second, and in a full table
  (10 000 addresses) a new address's failure isn't counted, so no one is locked out by
  another's flood. IPv6 is counted per /64; IPv4, and IPv4-mapped IPv6, per address (Oleksii).
- One counted verification, `Auth::verify_from(headers, Option<IpAddr>)`, refuses a waiting
  address, reserves the attempt under one lock, verifies, and resets on a success. `/v1`'s
  `authorize` and every `/ui/` route that verifies a caller use it, so the API's
  `Authorization: Basic` is limited too (`429` with `Retry-After`; the OpenAPI document's common
  responses gain `429`), and no `/ui/` route is a way around the limit (Oleksii: the API's Basic
  is limited in M14). Forwarded requests are counted (no node trust). A request answered `503`
  gives back the failure it reserved. Every `429` carries `Retry-After`.
- `safe_next` refuses a `next` with a `.` or `..` segment, percent-encoded ones too.
- The `/ui/` `401` pages carry `Session realm="data-avalanche"` (`Bearer` under `jwt`); the
  console-off `404` carries the hardening headers too.
- New tests: the escaping of the form's values; the hardening headers on a `303`, `401` and
  `429`; the table's cap, pruning, full table and per-/64 counting.
- Spec: the limit is a line in System-Overview's `basic` bullet (`b2a050d`, approved by
  Oleksii).

**Task 4** (`ada30f0`, one fix round):
- Merged by hand onto Tasks 1 and 3's amended code (`verify_from`, the cookie helper, the
  `Session` and `Bearer` challenges, the guard's CSP).
- A refresh refuses a session past `session_ttl`, or under another key or mode, before calling
  the provider: `401` with the `Session` challenge and the session's cookies removed (§6.8:
  from sign-in, however it is used).
- `Unavailable` is `503` on the refresh (JSON, like `GET /ui/session`) and on the callback (the
  sign-in page with an "unavailable" reason).
- The callback checks `state` before it shows a provider's `error`; an unparseable
  authorization endpoint is a `503` page; a too-long session after a refresh is `500`; a
  page's failed cookies after a refresh show the "ended" page with the reason.
- Discovery is cached by (discovery URL, issuer). The refresh is single-flight per refresh
  token (an in-flight map keyed by its digest), not one lock per node, so a provider that hangs
  holds up only its own sessions; the outcome is kept 30 s as planned.
- New tests: the issuer mismatch, `client_secret_post`, the refused ended-session refresh, the
  `503`s.

**Task 5** (`3fbcef7`, one fix round): the site and the research docs follow Tasks 1–4's
rulings. The console page is titled "Web console sign-in" and gains "Behind a proxy": the
console and `/v1` at the root of one host, the header size, a shared address's attempts.
Signing out removes the session's cookies and, with an end-session endpoint, signs out at the
provider.

**V-129** (`ae4247a` spec, `022355c` code, approved by Oleksii): `globals.console.public_url`
is an origin with no path (a trailing `/` aside), since the console's routes, redirects and
cookie paths are absolute `/ui/…`. Found by Task 5's review.

**Task 6** (`3fe1225`, one fix round): the E2E's provider checks what a real one does: the
code (single use), `S256(code_verifier)` against the challenge, and the `redirect_uri`, so the
scenario proves the binary sends them. `Proc::exchange_ok` joins the harness.

**The whole-branch review** and its fixes (`514cca9` spec, `73705d5`…`f9eb2f5`):
- Every stream re-checks its caller's credentials, one opened with
  `x-data-avalanche-forwarded-by` too: a client could send that header and keep a stream past its
  session (`73705d5`).
- A provider outage during a refresh (a transport error, a timeout, a `5xx`, `408` or `429`, a
  failed discovery) answers `503` with `Retry-After: 5` and keeps the session's cookies; a
  provider's refusal (any other `4xx`) ends the session (`401`) as before. An outage, and a failed
  discovery, are remembered 5 s; a success or a refusal 30 s (Oleksii).
- The refresh's in-flight map no longer leaks an entry when two refreshes of one token race to
  finish (Oleksii).
- The sign-in cookie is `__Host-da_oauth2` with `Path=/`, so a sibling subdomain can't plant one
  (Oleksii; `da_id` keeps its name).
- A node that hasn't run discovery runs it before building a page's CSP, so `form-action` names
  the provider's end-session origin.
- With no `Sec-Fetch-Site`, the cross-site check also accepts an `Origin` equal to `public_url`'s
  origin (a proxy that rewrites `Host`).
- OAuth2 sign-in, discovery and refresh failures are logged, never a token.
- The OpenAPI document's common `403` names the missing `X-CSRF-Token`.
- V-129 refuses user information in `public_url` (`514cca9` spec, approved by Oleksii;
  `d6a3a55` code).
- The E2E asserts `da_id`'s removal at sign-out.

**At the exit** (`f9eb2f5`, the last code commit): `cargo test --workspace` 1849 passed, both
clippy runs and `cargo fmt --all --check` clean, `cargo xtask docs --check` held; the whole E2E
suite passed on PostgreSQL 18 (59 tests, 842 s); Task 6's scenario passed on `mysql-84` (28 s),
`mysql-84-nogtid` (31 s) and `mariadb-118` (32 s); the site built (32 pages, link check clean).
