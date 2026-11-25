---
updated: 2026-11-25
domain: Architecture
tags: [v1.1, implementation-plan, m15, console, web-ui, svelte, overview, playwright, helm]
resolves: []
---

# M15 — The Console's Frame and Overview Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The 1.1.0 admin console's app (Admin_Console §4, §8, §10): the frame every page shares
and the Overview, served under `/ui/` from the binary itself, live through `/v1/stream`; and the
chart's console values and notes, so a cluster can switch the console on and find it.

**Architecture:**

- **The app's workspace** (Task 1, `ui/`): Svelte 5 with TypeScript, built by Vite to `ui/dist`
  (`base: '/ui/'`, the built files under `app/`), unit-tested by Vitest in jsdom. `xtask deps`
  tracks its packages and Node version as it tracks the site's.
- **Built into the binary** (Task 2, `data-avalanche-api`): `build.rs` embeds `ui/dist` when it
  holds a build; `GET /ui/app/{*file}` serves the files the page loads, kept by the browser for
  good; a page under `/ui/` serves the app's `index.html` to a caller who verifies, and says the
  app isn't built when it isn't. `DATA_AVALANCHE_UI=required` makes a build without it fail.
- **The frame** (Task 3, `ui/src/lib`, `Frame.svelte`, `App.svelte`): the session read from
  `GET /ui/session` and kept fresh (`POST /ui/session/refresh`); every `/v1` call through one
  client, which sends `X-CSRF-Token` with what changes something; the live stream (`Live`), with
  its state shown and a resync after a restart or a gap; the sections and the router.
- **The Overview** (Task 4, `ui/src/overview`): read part by part from `/v1`, then followed through
  the stream; tiles, "Needs attention" (computed in the page, with fixed thresholds it states),
  the pipelines worst first with the last hour's lag, and the newest events.
- **In a browser** (Task 5): Playwright specs (`ui/e2e`) driven by the E2E suite
  (`console_app.rs`): sign-in under every mode, the 401 and 403 pages, the Overview live, the
  console off, and the first page drawn within 1 s with 50 pipelines.
- **Shipped** (Task 6): the image builds the app first; CI checks, tests and builds it, and runs
  the browser flows; the chart's console values and notes; the site.

**Tech Stack:** Svelte 5.59.1, TypeScript 6.0.3 (held: svelte-check 4.7.8 takes `^5 || ^6`),
Vite 8.3.8 with `@sveltejs/vite-plugin-svelte` 7.3.2, Vitest 5.1.1 with jsdom 31.0.1,
svelte-check 4.7.8, `@playwright/test` 1.65.1 (Chromium), `@types/node` 26.6.10; Node 26
(`ui/.nvmrc`; the image's stage `node:26.13.1-trixie-slim`). No runtime npm dependency: the app is
Svelte's compiled output alone. Rust 1.100.0 (edition 2024), no new crate.

**Spec:**
- `doc/research/Admin_Console.md` §4 (the sections and the Overview), §4.1 (needs attention; the
  plan's research commit records the thresholds as built), §4.2 (charts), §8 and §8.1 (freshness,
  the page budgets, the live stream), §10 (front end and serving), §11 (the chart), §14.
- The approved mockup `doc/v1.1/mockups/01-overview.html` (the frame and the Overview).
- `doc/v1.1/M14-console-sessions.md` (the session, its refresh and the anti-forgery token, the
  pages the server renders), `M13f-console-stream.md` (the stream's messages and resets),
  `M13e-console-history.md` (`/v1/history`), `M13a-console-events.md`, `M13b-console-nodes.md`,
  `M13c-console-jobs.md`, `M13d-console-pipelines.md` (the reads the Overview makes).
- `doc/specs/Configuration_Specification.md` §6.8 (`globals.console`); no spec change.

## Global Constraints

- Toolchain `1.100.0`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`;
  - from Task 1, in `ui/`: `npm run check` (svelte-check, warnings fail it), `npm test`,
    `npm run build`.
- **Every release refreshes its dependencies** (CLAUDE.md): the app's packages are exact versions,
  each the newest stable one, or held in `dependency-exceptions.yaml` with the reason.
- **Tokens never reach page JavaScript** (Admin_Console §9): the page reads only what
  `GET /ui/session` gives (name, role, mode, the anti-forgery token, the token's expiry); the
  session's cookies are `HttpOnly`.
- **Under the console's Content-Security-Policy** (M14): `script-src 'self'; style-src 'self'`, so
  no inline script or style. The page is one module script and Svelte's extracted CSS.
- **Carried from M14:** a refresh that answers `503` (the provider unreachable) is retried after
  its `Retry-After`, never a sign-out; a `401` from the session, the refresh or any `/v1` call
  ends the page (`location.reload()`, and the server shows why). Every call that changes something
  sends `X-CSRF-Token`.
- **Now first, history second** (Admin_Console §8): a page renders the current state at once, then
  adds the chart's history; a part that fails to read keeps what it had and says so.
- **Page budgets** (Admin_Console §8): the first page drawn within 1 s with 50 pipelines on three
  nodes (Task 5 measures it); M16 adds the navigation budget.
- **The thresholds are fixed defaults, stated on the page** (Oleksii, 2026-11-25); a later release
  makes them configurable (DA-OPS-15).
- **Test gates:** plain `cargo test` and `npm test` stay hermetic. The browser flows need
  `DATA_AVALANCHE_IT=1`, `DATA_AVALANCHE_BROWSER=1`, the app built into the binary and Playwright's
  Chromium; they run in the E2E suite with `--test-threads=1`.
- **The site follows the behaviour in the same milestone** (`doc/v1.0/README.md`).
- **Code blocks:**
  - They are the verified content, already formatted (`rustfmt`; the TypeScript and Svelte as
    written, checked by svelte-check).
  - Every block is a `git apply`-ready diff, one per file.
  - A step's diffs apply in the order given, after the earlier steps'.
  - `ui/package-lock.json` is written by `npm install`, not by hand (Tasks 1 and 5 say the
    command).
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to hurt someone running 1.1.0 in production. Each is pinned by a
test in the task after the arrow.

1. **An identity provider that is down** while a page is open: the page must keep its session and
   try again, not sign the person out → Task 3 (`asks again after Retry-After when the provider is
   down, and never signs out for it`), Task 5 (`signing out ends the session; a session that no
   longer opens says why`).
2. **The stream going quiet or the node behind it restarting**: the page must say how old its
   numbers are, and read everything again rather than trust a stream that missed messages →
   Task 3 (`turns stale, then offline, as the messages stop, and reopens a silent stream`,
   `reads the state again after it restarts or misses messages: no replay`), Task 4 (`says when
   its numbers aren't current`).
3. **A rebalance or a rolling restart**: leases change hands for a few seconds, and a page that
   cries "no node running it" at each hand-off teaches people to ignore red → Task 4 (`keeps to
   its thresholds`: no line under 30 s without a holder; `remembers since when a lease has had no
   holder, until one holds it`).
4. **Fifty pipelines, some still starting**: the first page must still be drawn within 1 s, and a
   pipeline with no worker yet (its figures carry no source) must not stop the page → Task 4
   (`tiles and rows take a pipeline that is starting: no worker, no source yet`), Task 5
   (`the_first_page_is_drawn_within_its_budget`).
5. **A node the console can't follow**: its pipelines must show as last known, saying so, not as
   current → Task 4 (`rows of a node out of reach say it, and what was last known`), Task 3's
   stream (`peer` messages).

## Decisions and rulings

Settled with Oleksii before this plan (2026-11-25):
- the lag target is a **fixed 60 s**, stated on the page with every other threshold; a later
  release makes the thresholds configurable (DA-OPS-15);
- "Needs attention" is **computed in the page**, from the `/v1` reads and the stream;
- the Playwright **harness and M15's own flows** land now, in CI (sign-in per mode, the 401 and 403
  pages, the Overview live, the console off, the first-page budget); M16 adds its pages' flows and
  the navigation budget;
- the app is embedded by the API crate's **`build.rs`**, no new crate.

The rest are Claude's calls.

- **D-M15.1 The app's workspace** (`ui/`). Svelte 5 (runes; the stateful parts are `.svelte.ts`
  classes) and TypeScript, strict. Vite builds with `base: '/ui/'` and `assetsDir: 'app'`, so the
  page's files are `/ui/app/<name>-<hash>.<ext>`. In test mode Vite resolves Svelte's `browser`
  condition (Vitest in jsdom mounts components). No router or UI library: the frame's router is
  thirty lines over the History API. Only devDependencies, all exact versions; TypeScript held at
  6 by svelte-check. `xtask deps` reads `ui/package.json`'s devDependencies and `ui/.nvmrc`.
- **D-M15.2 Built into the binary** (`build.rs`, `app.rs`, `console.rs`).
  - `build.rs` walks `ui/dist` when it holds `index.html` and writes `$OUT_DIR/app.rs`, a table of
    `AppFile { path, body: include_bytes!(…) }`. It reruns when `ui/dist` changes, or, while there
    is none, when `ui/` does (a path that doesn't exist would rerun it at every build).
  - `DATA_AVALANCHE_UI=required` (CI, the image) panics without the app. Unset, a build without it
    serves `pages::not_built` to a caller who verifies: who they are, and how to build the app.
    Rust-only work needs no Node.
  - `Api.app: App` (`App::built()` in the binary, `App::of(&[])` in tests). `GET /ui/app/{*file}`
    answers only a path in the table, to anyone (code, no data), with `Cache-Control: public,
    max-age=31536000, immutable`: a new build names its files anew. The page itself goes through
    M14's verification and headers like every page under `/ui/`.
- **D-M15.3 The session in the page** (`session.ts`). `readSession` reads `GET /ui/session`; a
  `401` there ends the page. `keepFresh` refreshes an `oauth2` token `REFRESH_AHEAD_MS` (60 s)
  before it expires, with the anti-forgery token; a `503` is asked again after `Retry-After`
  (else `RETRY_MS`, 5 s); a `401` ends the page; other modes have nothing to refresh.
- **D-M15.4 The `/v1` client** (`api.ts`). `get` reads JSON within `READ_WITHIN_MS` (5 s,
  `AbortSignal.timeout`); `send` adds `X-CSRF-Token` when the session has one. A `401` ends the
  page; any other failure is an `HttpError` with the server's own words (its JSON `error`) for
  the page to show.
- **D-M15.5 The live stream** (`live.svelte.ts`). One `EventSource` on `/v1/stream`, its messages
  handed out by kind.
  - State: `connecting`, `live`, `reconnecting`, and `offline` once no message came for
    `OFFLINE_AFTER_MS` (30 s); a stream silent for `STALE_AFTER_MS` (10 s) is reopened.
  - Resync: a second `hello` (a restart) or a `reset` for `lagged` (missed messages) makes every
    reader read again, since the stream has no replay.
  - A `reset` for credentials, or a source the browser closed, checks the session first (a `401`
    ends the page), then reopens after 1, 2, 5, then 10 s (`REOPEN_MS`); "Retry now" reopens at
    once.
- **D-M15.6 The frame** (`Frame.svelte`, `router.svelte.ts`, `App.svelte`). The sections of
  mockup 01 (Overview, Cluster, Pipelines, Tables, Snapshots, Events, Config); those M16 builds say
  "This page isn't in this build yet." The bar shows the connection's state (`reconnecting · last
  update N s ago`), `config gen N` (from `/v1/config/status`), the name and role, and the sign-out
  (a form, so M14's server handles it) when there is a session. `none` shows M14's banner. Offline,
  an alert says since when everything is as of, with "Retry now". A plain click on an app link
  under `/ui/` is routed in the page; the server's own routes (`/ui/sign-in`, `/ui/sign-out`,
  `/ui/session`, `/ui/oauth2/`, `/ui/assets/`, `/ui/app/`) are not.
- **D-M15.7 The Overview's data** (`model.svelte.ts`). Eight parts, each read on its own, so one
  failing keeps the others: `/v1/pipelines`, `/v1/cluster/status`, `/v1/config/nodes`,
  `/v1/snapshots`, `/v1/tables`, `/v1/events?limit=100`, the newest pause, stop or resume of each
  pipeline (`/v1/events?kind=action.pipeline.pause,…&limit=1000`), and the last hour's lag
  (`/v1/history?series=lag_seconds`, every pipeline). Then the stream: `pipeline` and `figures`
  (the higher `owner_epoch` wins, since a killed node sends no `standby`; a pipeline with no
  worker, stopped or starting, has figures with no `worker` and no `source`), `samples` (the lag line
  keeps the last hour), `event` (once by key; the parts it changes read again, a burst at once
  after `REREAD_AFTER_MS`, 1 s), `members`, and `peer` (a node out of reach, since when).
- **D-M15.8 Needs attention** (`attention.ts`): red, then orange, then yellow, the newest first, each
  linking where it's dealt with. The thresholds, as `Admin_Console.md` §4.1 now records them: lag
  target 60 s, rising over 5 min; a slot's WAL past 50% of `max_slot_wal_keep_size`, or 16 GiB
  without one; paused or stopped by a person over 1 h; snapshot jobs failed within 24 h; lease
  losses within 1 h; a lease with no holder (or one that stopped renewing) for over 30 s, the page
  remembering since when it saw it (`Facts.unheld`), so a rebalance's hand-offs don't show. "Repeated
  lease-renewal failures" is read as a `lease.lost` or `lease.takeover` within the hour; "sink
  commit failures" as the worker reporting itself degraded. **A pending removal (tombstone)** isn't
  a rule here: no `/v1` route reads `removals/` (DA-API-21); M16 adds one with the Cluster page,
  then the rule.
- **D-M15.9 The Overview page** (`derive.ts`, `Overview.svelte`, `Sparkline.svelte`): mockup 01's
  tiles (Pipelines by state, Worst lag against the target, Nodes up and their config generation,
  Snapshots running and failed today, WAL held by slots); the pipelines worst first (state chip,
  node, the last hour's lag as a sparkline against the target, last commit), a node out of reach
  marking its rows `unknown · nX unreachable N s · last known: …`; the eight newest events; the
  thresholds at the foot. A part that failed says so; numbers older than the stream's offline
  limit say how old.
- **D-M15.10 The browser flows** (`ui/e2e`, `console_app.rs`). The E2E suite starts the nodes and
  runs `npx playwright test <spec>` in `ui/` with the node's URL and what the spec needs
  (`DA_CONSOLE_URL`, a user and password, tokens). One worker, Chromium. The test provider
  (`idp.rs`) answers a browser's `/authorize` with its approval, as `approve` does for the Rust
  scenario. The budget runs 50 pipelines on three nodes, so the PostgreSQL services allow 64 slots
  and WAL senders and 300 connections; it waits until every pipeline has a holder, warms the
  browser's cache of the app's files once (a returning person's), then takes the time since
  navigation in the page, at the first frame that shows the 50 rows and the Pipelines tile.
  The page waits on `/v1/pipelines`, which read its pipelines one after another (650–1 010 ms
  for 50 locally, more on a cloud store); it now reads them 16 at a time (`READ_AT_ONCE`, as
  `/v1/snapshots` and `/v1/tables` do): the page was drawn in 220–661 ms over five runs.
- **D-M15.11 Shipped.** The image's first stage (`node:26.13.1-trixie-slim`) runs `npm ci` and
  `npm run build`; the build stage copies `ui/dist` and sets `DATA_AVALANCHE_UI=required`. CI's
  check job builds and tests the app first, with `DATA_AVALANCHE_UI=required` for every cargo
  step; the integration job builds it, installs Chromium and sets `DATA_AVALANCHE_BROWSER=1`. The
  chart gains no switch (the console is in the config, §11): `values.yaml`'s comments show the
  block, `ci/console-values.yaml` is a console-on case (OAuth2, the session key and client secret
  from a Secret) that `packaging.rs` validates and CI renders, and `NOTES.txt` gives the console's
  address: `public_url` + `/ui/` when set, else through the port-forward.
- **D-M15.12 Rollback.** Nothing is stored. A binary rolled back to M14 serves M14's landing page
  again; the browser's cached app files are never asked for.

## File Structure

| File | Responsibility |
|---|---|
| `ui/{package.json,package-lock.json,.nvmrc,tsconfig.json,vite.config.ts,svelte.config.js,index.html}` (new), `.gitignore`, `dependency-exceptions.yaml`, `xtask/src/deps.rs` | The app's workspace, tracked by `xtask deps` (Task 1); Playwright (Task 5) |
| `ui/src/{main.ts,App.svelte,app.css}` (new), `ui/src/lib/format.ts` (new) | The entry, the page's styles, the formats (Task 1); the app's wiring (Tasks 3, 4) |
| `crates/data-avalanche-api/build.rs` (new), `src/app.rs` (new), `src/{lib,console,pages}.rs`, every `Api {` (`config.rs`, `generations.rs`, `server.rs`, `test_support.rs`, `crates/data-avalanche/src/run.rs`) | The app built in and served (Task 2) |
| `ui/src/lib/{session,api,live.svelte,router.svelte}.ts` (new), `ui/src/{Frame,Later}.svelte` (new) | The frame (Task 3) |
| `ui/src/overview/*` (new) | The Overview (Task 4) |
| `ui/playwright.config.ts`, `ui/e2e/*` (new), `crates/data-avalanche/tests/e2e/{console_app,main,idp,console_sessions}.rs`, `test-infra/compose.yaml`, `crates/data-avalanche-api/src/pipelines.rs` | The browser flows; `/v1/pipelines` read at once (Task 5) |
| `deploy/docker/Dockerfile`, `.dockerignore`, `.github/workflows/ci.yml`, `deploy/helm/data-avalanche/{values.yaml,README.md,templates/NOTES.txt,ci/console-values.yaml}`, `crates/data-avalanche/tests/packaging.rs`, `docs/…/deployment/{console,container,kubernetes}.md`, `test-infra/README.md`, `doc/v1.0/README.md` | Shipped (Task 6) |

The plan's commit also carries `doc/v1.1/README.md` (M14 merged, #33; this plan's row). Two more
commits go with it: `research: Admin_Console — §4.1 the thresholds as built` and `gap: v1 §70 —
1.1.0 M15's plan, DA-API-21 and DA-OPS-15`.

---

### Task 1: The app's workspace (`ui/`, `xtask`)

**Files:**
- Modify: `.gitignore`
- Modify: `dependency-exceptions.yaml`
- Create: `ui/.nvmrc`
- Create: `ui/index.html`
- Regenerate: `ui/package-lock.json`
- Create: `ui/package.json`
- Create: `ui/src/App.svelte`
- Create: `ui/src/app.css`
- Create: `ui/src/lib/format.test.ts`
- Create: `ui/src/lib/format.ts`
- Create: `ui/src/main.ts`
- Create: `ui/svelte.config.js`
- Create: `ui/tsconfig.json`
- Create: `ui/vite.config.ts`
- Modify: `xtask/src/deps.rs`

**Interfaces:**
- Consumes: nothing earlier.
- Produces: the `ui/` workspace (D-M15.1): `npm run build` writes `ui/dist` (`index.html`,
  `app/*`), `npm run check`, `npm test`, `npm run e2e`; `ui/.nvmrc` (`26`).
- Produces (`ui/src/lib/format.ts`): `duration(seconds: number): string` (two largest units,
  `4m 12s`), `ago(then: number, now: number): string`, `bytes(n: number): string` (1024s, as
  PostgreSQL writes them: `3.2 GB`), `clock(at: number, seconds = false): string`.
- Produces (`xtask/src/deps.rs`): `npm()` reads `docs/package.json`'s `dependencies` and
  `ui/package.json`'s `devDependencies`, each with its `.nvmrc`.

- [ ] **Step 1: Write the failing tests**

The tests: `the console writes` (`format.test.ts`: a duration, how long ago, a size, the time of
day); `the_inventory_covers_every_kind` asserts `has(Kind::Npm, "svelte")`. The workspace's files
come with them, since the tests need them to run; then `npm install` in `ui/` writes
`ui/package-lock.json`.

`.gitignore`:

````diff
diff --git a/.gitignore b/.gitignore
index 4beca48..07f7b63 100644
--- a/.gitignore
+++ b/.gitignore
@@ -2,3 +2,7 @@ target/
 prototypes/*/target/
 test-infra/.tls/
 test-infra/.k3s/
+ui/node_modules/
+ui/dist/
+ui/test-results/
+ui/playwright-report/
````

`dependency-exceptions.yaml`:

````diff
diff --git a/dependency-exceptions.yaml b/dependency-exceptions.yaml
index e45d12f..99a2680 100644
--- a/dependency-exceptions.yaml
+++ b/dependency-exceptions.yaml
@@ -12,6 +12,7 @@
 - { kind: crate, name: arrow-row, held: "59", reason: "deltalake-core 1.1.4 requires arrow ^59; one arrow major workspace-wide, D-MS.14" }
 - { kind: crate, name: arrow-schema, held: "59", reason: "deltalake-core 1.1.4 requires arrow ^59; one arrow major workspace-wide, D-MS.14" }
 - { kind: crate, name: arrow-select, held: "59", reason: "deltalake-core 1.1.4 requires arrow ^59; one arrow major workspace-wide, D-MS.14" }
+- { kind: npm, name: typescript, held: "6", reason: "svelte-check 4.7.8, the newest, takes typescript ^5 or ^6" }
 - { kind: crate, name: mysql_common, held: "0.37", reason: "mysql_async 0.37.1, the newest, requires mysql_common ^0.37" }
 - { kind: image, name: postgres, held: "*", reason: "the source support matrix, not a build dependency; widening it is a feature" }
 - { kind: image, name: mysql, held: "*", reason: "the source support matrix, not a build dependency; widening it is a feature" }
````

`ui/.nvmrc`:

````diff
diff --git a/ui/.nvmrc b/ui/.nvmrc
new file mode 100644
index 0000000..6f4247a
--- /dev/null
+++ b/ui/.nvmrc
@@ -0,0 +1 @@
+26
````

`ui/index.html`:

````diff
diff --git a/ui/index.html b/ui/index.html
new file mode 100644
index 0000000..41fb084
--- /dev/null
+++ b/ui/index.html
@@ -0,0 +1,13 @@
+<!doctype html>
+<html lang="en">
+  <head>
+    <meta charset="utf-8" />
+    <meta name="viewport" content="width=device-width, initial-scale=1" />
+    <meta name="color-scheme" content="light dark" />
+    <title>data-avalanche</title>
+  </head>
+  <body>
+    <div id="app"></div>
+    <script type="module" src="/src/main.ts"></script>
+  </body>
+</html>
````

`ui/package.json`:

````diff
diff --git a/ui/package.json b/ui/package.json
new file mode 100644
index 0000000..f5d5c89
--- /dev/null
+++ b/ui/package.json
@@ -0,0 +1,21 @@
+{
+  "name": "data-avalanche-console",
+  "private": true,
+  "type": "module",
+  "scripts": {
+    "build": "vite build",
+    "check": "svelte-check --tsconfig ./tsconfig.json --fail-on-warnings",
+    "test": "vitest run",
+    "e2e": "playwright test"
+  },
+  "devDependencies": {
+    "@playwright/test": "1.65.1",
+    "@sveltejs/vite-plugin-svelte": "7.3.2",
+    "jsdom": "31.0.1",
+    "svelte": "5.59.1",
+    "svelte-check": "4.7.8",
+    "typescript": "6.0.3",
+    "vite": "8.3.8",
+    "vitest": "5.1.1"
+  }
+}
````

`ui/src/App.svelte`:

````diff
diff --git a/ui/src/App.svelte b/ui/src/App.svelte
new file mode 100644
index 0000000..60f3d3a
--- /dev/null
+++ b/ui/src/App.svelte
@@ -0,0 +1,6 @@
+<script lang="ts">
+</script>
+
+<main>
+  <h1>data-avalanche</h1>
+</main>
````

`ui/src/app.css`:

````diff
diff --git a/ui/src/app.css b/ui/src/app.css
new file mode 100644
index 0000000..a8bc41a
--- /dev/null
+++ b/ui/src/app.css
@@ -0,0 +1,3 @@
+:root {
+  font: 13px/1.45 system-ui, -apple-system, 'Segoe UI', sans-serif;
+}
````

`ui/src/lib/format.test.ts`:

````diff
diff --git a/ui/src/lib/format.test.ts b/ui/src/lib/format.test.ts
new file mode 100644
index 0000000..b6372e5
--- /dev/null
+++ b/ui/src/lib/format.test.ts
@@ -0,0 +1,34 @@
+import { describe, expect, it } from 'vitest';
+import { ago, bytes, clock, duration } from './format';
+
+describe('the console writes', () => {
+  it('a duration in its two largest units', () => {
+    expect(duration(0)).toBe('0 s');
+    expect(duration(21.4)).toBe('21 s');
+    expect(duration(252)).toBe('4m 12s');
+    expect(duration(7200)).toBe('2h 0m');
+    expect(duration(90061)).toBe('1d 1h');
+  });
+
+  it('how long ago, from now', () => {
+    const now = Date.UTC(2026, 10, 25, 14, 0, 0);
+    expect(ago(now - 8_000, now)).toBe('8 s ago');
+    expect(ago(now - 180_000, now)).toBe('3 min ago');
+    expect(ago(now - 7_200_000, now)).toBe('2 h ago');
+    expect(ago(now - 3 * 86_400_000, now)).toBe('3 d ago');
+    // A clock a little ahead of the browser's isn't in the future.
+    expect(ago(now + 2_000, now)).toBe('0 s ago');
+  });
+
+  it('a size as PostgreSQL does, in 1024s', () => {
+    expect(bytes(512)).toBe('512 bytes');
+    expect(bytes(3.4 * 1024 ** 3)).toBe('3.4 GB');
+    expect(bytes(512 * 1024 ** 2)).toBe('512 MB');
+    expect(bytes(1536)).toBe('1.5 kB');
+  });
+
+  it('the time of day, hours and minutes', () => {
+    const at = new Date(2026, 10, 25, 9, 5, 30).getTime();
+    expect(clock(at)).toBe('09:05');
+  });
+});
````

`ui/src/main.ts`:

````diff
diff --git a/ui/src/main.ts b/ui/src/main.ts
new file mode 100644
index 0000000..2b99320
--- /dev/null
+++ b/ui/src/main.ts
@@ -0,0 +1,5 @@
+import { mount } from 'svelte';
+import App from './App.svelte';
+import './app.css';
+
+mount(App, { target: document.getElementById('app')! });
````

`ui/svelte.config.js`:

````diff
diff --git a/ui/svelte.config.js b/ui/svelte.config.js
new file mode 100644
index 0000000..36c98fd
--- /dev/null
+++ b/ui/svelte.config.js
@@ -0,0 +1,3 @@
+import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';
+
+export default { preprocess: vitePreprocess() };
````

`ui/tsconfig.json`:

````diff
diff --git a/ui/tsconfig.json b/ui/tsconfig.json
new file mode 100644
index 0000000..7d7bcf2
--- /dev/null
+++ b/ui/tsconfig.json
@@ -0,0 +1,30 @@
+{
+  "compilerOptions": {
+    "target": "ES2022",
+    "module": "ESNext",
+    "moduleResolution": "bundler",
+    "lib": [
+      "ES2022",
+      "DOM",
+      "DOM.Iterable"
+    ],
+    "strict": true,
+    "noEmit": true,
+    "isolatedModules": true,
+    "verbatimModuleSyntax": true,
+    "skipLibCheck": true,
+    "types": [
+      "svelte",
+      "vite/client"
+    ],
+    "moduleDetection": "force",
+    "allowJs": true
+  },
+  "include": [
+    "src/**/*.ts",
+    "src/**/*.svelte",
+    "e2e/**/*.ts",
+    "vite.config.ts",
+    "playwright.config.ts"
+  ]
+}
````

`ui/vite.config.ts`:

````diff
diff --git a/ui/vite.config.ts b/ui/vite.config.ts
new file mode 100644
index 0000000..c0c6ca3
--- /dev/null
+++ b/ui/vite.config.ts
@@ -0,0 +1,11 @@
+import { svelte } from '@sveltejs/vite-plugin-svelte';
+import { defineConfig } from 'vitest/config';
+
+// The node serves the app under /ui/ (Admin_Console §10), its built files under /ui/app/: the
+// server's own pages keep /ui/assets/.
+export default defineConfig({
+  base: '/ui/',
+  plugins: [svelte()],
+  build: { assetsDir: 'app', target: 'es2022' },
+  test: { environment: 'jsdom', include: ['src/**/*.test.ts'] },
+});
````

`xtask/src/deps.rs`:

````diff
diff --git a/xtask/src/deps.rs b/xtask/src/deps.rs
index f700249..cfed8e3 100644
--- a/xtask/src/deps.rs
+++ b/xtask/src/deps.rs
@@ -1071,6 +1071,7 @@ mod tests {
         assert!(has(Kind::Tool, "helm"));
         assert!(has(Kind::Tool, "duckdb"));
         assert!(has(Kind::Npm, "astro"));
+        assert!(has(Kind::Npm, "svelte"));
         assert!(has(Kind::Node, "node"));
         assert!(
             items
````

- [ ] **Step 2: Run them to see them fail**

Run: `(cd ui && npm install && npm test); cargo test -p xtask deps::`
Expected: `npm test` fails: `Failed to resolve import "./format" from "src/lib/format.test.ts"`; `the_inventory_covers_every_kind` fails at `assert!(has(Kind::Npm, "svelte"))` (28 others pass).

- [ ] **Step 3: Implement**

Write `format.ts`, and let `npm()` read both workspaces:

`ui/src/lib/format.ts`:

````diff
diff --git a/ui/src/lib/format.ts b/ui/src/lib/format.ts
new file mode 100644
index 0000000..545e408
--- /dev/null
+++ b/ui/src/lib/format.ts
@@ -0,0 +1,39 @@
+// How the console writes times and sizes: short, in the units a person reads at a glance.
+
+/** A duration in its two largest units: `21 s`, `4m 12s`, `2h 0m`, `1d 1h`. */
+export function duration(seconds: number): string {
+  const s = Math.max(0, Math.round(seconds));
+  if (s < 60) return `${s} s`;
+  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
+  if (s < 86_400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
+  return `${Math.floor(s / 86_400)}d ${Math.floor((s % 86_400) / 3600)}h`;
+}
+
+/** How long before `now` (both in ms) `then` was: `8 s ago`, `3 min ago`, `2 h ago`. */
+export function ago(then: number, now: number): string {
+  const s = Math.max(0, Math.floor((now - then) / 1000));
+  if (s < 60) return `${s} s ago`;
+  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
+  if (s < 86_400) return `${Math.floor(s / 3600)} h ago`;
+  return `${Math.floor(s / 86_400)} d ago`;
+}
+
+const UNITS = ['kB', 'MB', 'GB', 'TB', 'PB'];
+
+/** A size as PostgreSQL's `pg_size_pretty` writes it, in 1024s: `512 bytes`, `3.4 GB`. */
+export function bytes(n: number): string {
+  if (n < 1024) return `${Math.round(n)} bytes`;
+  let v = n / 1024;
+  let unit = 0;
+  while (v >= 1024 && unit < UNITS.length - 1) {
+    v /= 1024;
+    unit += 1;
+  }
+  return `${v < 10 ? v.toFixed(1).replace(/\.0$/, '') : Math.round(v)} ${UNITS[unit]}`;
+}
+
+/** The local time of day of `at` (ms): `09:05`. */
+export function clock(at: number): string {
+  const d = new Date(at);
+  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
+}
````

`xtask/src/deps.rs`:

````diff
diff --git a/xtask/src/deps.rs b/xtask/src/deps.rs
index cfed8e3..5387837 100644
--- a/xtask/src/deps.rs
+++ b/xtask/src/deps.rs
@@ -602,27 +602,32 @@ fn duckdb(root: &Path, items: &mut Vec<Item>) -> Result<(), String> {
     Ok(())
 }
 
+/// The site's packages (`docs/`) and the console's (`ui/`), each with its Node version.
 fn npm(root: &Path, items: &mut Vec<Item>) -> Result<(), String> {
-    let text = read(root, "docs/package.json")?;
-    let json: serde_json::Value =
-        serde_json::from_str(&text).map_err(|e| format!("docs/package.json: {e}"))?;
-    for (name, version) in json["dependencies"].as_object().into_iter().flatten() {
+    for (dir, section) in [("docs", "dependencies"), ("ui", "devDependencies")] {
+        let file = format!("{dir}/package.json");
+        let text = read(root, &file)?;
+        let json: serde_json::Value =
+            serde_json::from_str(&text).map_err(|e| format!("{file}: {e}"))?;
+        for (name, version) in json[section].as_object().into_iter().flatten() {
+            items.push(Item {
+                kind: Kind::Npm,
+                name: name.clone(),
+                current: version.as_str().unwrap_or_default().to_string(),
+                pinned_in: file.clone(),
+            });
+        }
+        let nvmrc = format!("{dir}/.nvmrc");
         items.push(Item {
-            kind: Kind::Npm,
-            name: name.clone(),
-            current: version.as_str().unwrap_or_default().to_string(),
-            pinned_in: "docs/package.json".into(),
+            kind: Kind::Node,
+            name: "node".into(),
+            current: read(root, &nvmrc)?
+                .trim()
+                .trim_start_matches('v')
+                .to_string(),
+            pinned_in: nvmrc,
         });
     }
-    items.push(Item {
-        kind: Kind::Node,
-        name: "node".into(),
-        current: read(root, "docs/.nvmrc")?
-            .trim()
-            .trim_start_matches('v')
-            .to_string(),
-        pinned_in: "docs/.nvmrc".into(),
-    });
     Ok(())
 }
 
````

- [ ] **Step 4: Run the tests and the gates**

Run: `(cd ui && npm install && npm test); cargo test -p xtask deps::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
(cd ui && npm run check && npm test && npm run build)
```

Expected: no output from the first three; every test passes; svelte-check reports 0 errors and
0 warnings.

- [ ] **Step 5: Commit**

```bash
git add .gitignore dependency-exceptions.yaml ui/.nvmrc ui/index.html ui/package-lock.json ui/package.json ui/src/App.svelte ui/src/app.css ui/src/lib/format.test.ts ui/src/lib/format.ts ui/src/main.ts ui/svelte.config.js ui/tsconfig.json ui/vite.config.ts xtask/src/deps.rs
git commit -m "build(ui): the console's app workspace — Svelte 5, Vite, Vitest; xtask deps tracks it"
```

---

### Task 2: The app built into the binary (`data-avalanche-api`)

**Files:**
- Create: `crates/data-avalanche-api/build.rs`
- Create: `crates/data-avalanche-api/src/app.rs`
- Modify: `crates/data-avalanche-api/src/config.rs`
- Modify: `crates/data-avalanche-api/src/console.rs`
- Modify: `crates/data-avalanche-api/src/generations.rs`
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Modify: `crates/data-avalanche-api/src/pages.rs`
- Modify: `crates/data-avalanche-api/src/server.rs`
- Modify: `crates/data-avalanche-api/src/test_support.rs`
- Modify: `crates/data-avalanche/src/run.rs`
- Modify: `crates/data-avalanche/tests/e2e/console_sessions.rs`

**Interfaces:**
- Consumes: Task 1's `ui/dist`; M14's `console::page`, `pages`, `Frame`, the guard.
- Produces (crate root): `App` (`App::built()`, `App::of(&'static [AppFile])`), `AppFile { path,
  body }`; `Api.app: App`. Crate-private: `App::index() -> Option<&'static [u8]>`,
  `App::file(name) -> Option<(&'static [u8], &'static str)>`; `pages::not_built(frame, name,
  role, session)` (M14's `signed_in`, renamed, saying how to build the app).
- Produces (`build.rs`): `$OUT_DIR/app.rs`; `DATA_AVALANCHE_UI=required` refuses a build without
  the app.

- [ ] **Step 1: Write the failing tests**

The tests (`console.rs`): `a_signed_in_caller_gets_the_app_at_every_page`,
`the_app_s_files_are_served_to_anyone_and_kept`,
`without_the_app_a_signed_in_caller_is_told_it_isn_t_built`; the console-off test's paths gain
`/ui/app/index-x.js`. Every `Api {` gains `app`, and `pages::signed_in` becomes `not_built`.

`crates/data-avalanche-api/src/config.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/config.rs b/crates/data-avalanche-api/src/config.rs
index eb2851a..d0d9cd1 100644
--- a/crates/data-avalanche-api/src/config.rs
+++ b/crates/data-avalanche-api/src/config.rs
@@ -226,6 +226,7 @@ mod tests {
             ready: Arc::new(AtomicBool::new(true)),
             node,
             auth: crate::test_support::open(),
+            app: crate::App::of(&[]),
         })
         .await;
         let check = |text: String| async move {
@@ -382,6 +383,7 @@ mod tests {
             ready: Arc::new(AtomicBool::new(true)),
             node,
             auth: crate::test_support::open(),
+            app: crate::App::of(&[]),
         })
         .await;
 
````

`crates/data-avalanche-api/src/console.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/console.rs b/crates/data-avalanche-api/src/console.rs
index 274a24f..159d4e0 100644
--- a/crates/data-avalanche-api/src/console.rs
+++ b/crates/data-avalanche-api/src/console.rs
@@ -721,12 +721,12 @@ mod tests {
     use serde_json::Value;
 
     use super::*;
-    use crate::Auth;
     use crate::session::SESSION_COOKIE;
     use crate::test_support::{
         Issuer, api, basic_api, basic_session, config, console, request, serve, session_cookie,
         sessions,
     };
+    use crate::{App, AppFile, Auth};
 
     const HOUR: Duration = Duration::from_secs(3600);
 
@@ -772,6 +772,7 @@ mod tests {
             "/ui/sign-in",
             "/ui/session",
             "/ui/assets/console.css",
+            "/ui/app/index-x.js",
         ] {
             let (code, _, body) = request(addr, "GET", path, &[], "").await;
             assert_eq!(code, 404, "{path}");
@@ -1433,4 +1434,104 @@ mod tests {
             assert_eq!(request(addr, "GET", other, &[], "").await.0, 404, "{other}");
         }
     }
+
+    /// The built app of the tests below: its page, and a file it loads.
+    static APP: &[AppFile] = &[
+        AppFile {
+            path: "app/index-x.js",
+            body: b"export {};",
+        },
+        AppFile {
+            path: "index.html",
+            body: b"<!doctype html><title>the app</title>",
+        },
+    ];
+
+    /// [`node`], with [`APP`] built in.
+    async fn node_with_app(api_config: &data_avalanche_config::Api, c: &Console) -> SocketAddr {
+        let mut a = api(Arc::new(Auth::new(api_config, c)));
+        a.app = App::of(APP);
+        serve(a).await
+    }
+
+    /// Admin_Console §10: every page under `/ui/` is the app's page for a caller who verifies;
+    /// the server's own pages still answer the others.
+    #[tokio::test]
+    async fn a_signed_in_caller_gets_the_app_at_every_page() {
+        let addr = node_with_app(&basic_api("s3cret"), &sessions(8 * HOUR)).await;
+        let (code, head, _) = request(addr, "GET", "/ui/pipelines/orders", &[], "").await;
+        assert_eq!(
+            (code, header_of(&head, "location")),
+            (303, vec!["/ui/sign-in?next=%2Fui%2Fpipelines%2Forders"])
+        );
+        let form = "user=ops&password=s3cret";
+        let (_, head, _) = request(addr, "POST", "/ui/sign-in", &[], form).await;
+        let cookie = kept(&head);
+        for path in ["/ui/", "/ui/pipelines/orders?tab=tables"] {
+            let with = [("Cookie", cookie.as_str())];
+            let (code, head, page) = request(addr, "GET", path, &with, "").await;
+            assert_eq!(
+                (code, page.as_str()),
+                (200, "<!doctype html><title>the app</title>"),
+                "{path}"
+            );
+            assert_eq!(
+                header_of(&head, "content-type"),
+                ["text/html; charset=utf-8"]
+            );
+            assert_eq!(header_of(&head, "cache-control"), ["no-store"]);
+            assert_eq!(header_of(&head, "x-frame-options"), ["DENY"]);
+        }
+        // A session that no longer opens: the server's page says why, not the app.
+        let ended = format!("{SESSION_COOKIE}=x");
+        let (code, _, page) = request(addr, "GET", "/ui/", &[("Cookie", &ended)], "").await;
+        assert_eq!(code, 401);
+        assert!(!page.contains("the app"), "{page}");
+    }
+
+    /// Admin_Console §10: the files the app loads answer anyone (code, no data) and stay in the
+    /// browser's cache, since a new build names them anew; a name not built is 404.
+    #[tokio::test]
+    async fn the_app_s_files_are_served_to_anyone_and_kept() {
+        let addr = node_with_app(&basic_api("s3cret"), &sessions(8 * HOUR)).await;
+        let (code, head, body) = request(addr, "GET", "/ui/app/index-x.js", &[], "").await;
+        assert_eq!((code, body.as_str()), (200, "export {};"));
+        assert_eq!(
+            header_of(&head, "content-type"),
+            ["text/javascript; charset=utf-8"]
+        );
+        assert_eq!(
+            header_of(&head, "cache-control"),
+            ["public, max-age=31536000, immutable"]
+        );
+        assert_eq!(header_of(&head, "x-content-type-options"), ["nosniff"]);
+        for path in [
+            "/ui/app/index-y.js",
+            "/ui/app/index.html",
+            "/ui/app/../index.html",
+        ] {
+            assert_eq!(request(addr, "GET", path, &[], "").await.0, 404, "{path}");
+        }
+    }
+
+    /// Admin_Console §10: a binary built without the app (Rust-only work needs no Node) tells a
+    /// signed-in caller so, and how to build it.
+    #[tokio::test]
+    async fn without_the_app_a_signed_in_caller_is_told_it_isn_t_built() {
+        let addr = node(&basic_api("s3cret"), &sessions(8 * HOUR)).await;
+        let form = "user=ops&password=s3cret";
+        let (_, head, _) = request(addr, "POST", "/ui/sign-in", &[], form).await;
+        let cookie = kept(&head);
+        let (code, _, page) = request(addr, "GET", "/ui/", &[("Cookie", &cookie)], "").await;
+        assert_eq!(code, 200);
+        assert!(
+            page.contains("isn't built into this binary")
+                && page.contains("<code>npm ci &amp;&amp; npm run build</code>"),
+            "{page}"
+        );
+        assert_eq!(
+            request(addr, "GET", "/ui/app/index-x.js", &[], "").await.0,
+            404
+        );
+    }
 }
````

`crates/data-avalanche-api/src/generations.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/generations.rs b/crates/data-avalanche-api/src/generations.rs
index 3897be7..aeddd09 100644
--- a/crates/data-avalanche-api/src/generations.rs
+++ b/crates/data-avalanche-api/src/generations.rs
@@ -302,6 +302,7 @@ mod tests {
             ready: Arc::new(AtomicBool::new(true)),
             node,
             auth: open(),
+            app: crate::App::of(&[]),
         })
         .await;
         let (code, v) = json_of(addr, "GET", "/v1/config/generations?limit=100").await;
@@ -336,6 +337,7 @@ mod tests {
             ready: Arc::new(AtomicBool::new(true)),
             node,
             auth: open(),
+            app: crate::App::of(&[]),
         })
         .await;
         std::fs::write(
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 97601bb..b64fe22 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -11,6 +11,7 @@
 //! ([`CSRF_HEADER`]) when it changes something: without it, 403, before its role is looked at.
 
 mod actions;
+mod app;
 mod auth;
 mod cluster;
 mod config;
@@ -59,6 +60,7 @@ use serde_json::{Value, json};
 use tokio::sync::watch;
 
 pub use actions::{DENIALS_BURST, MAX_ERROR, MAX_NOTE, MAX_TARGET, Recorded, note_of};
+pub use app::{App, AppFile};
 pub use auth::{
     Auth, CSRF_HEADER, FETCH_TIMEOUT, KEYS_MAX_AGE, MAX_KEY_DOCUMENT, Principal, REFETCH_AFTER,
 };
@@ -98,6 +100,8 @@ pub struct Api {
     /// Set once the node runs its config generation; the `/v1` routes answer 503 before.
     pub node: Arc<OnceLock<Node>>,
     pub auth: Arc<Auth>,
+    /// The console's app, served under `/ui/`: [`App::built`] in a node.
+    pub app: App,
 }
 
 /// What the `/v1` routes act on, once the node runs its config generation.
````

`crates/data-avalanche-api/src/pages.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/pages.rs b/crates/data-avalanche-api/src/pages.rs
index 8abc38e..b1c2c32 100644
--- a/crates/data-avalanche-api/src/pages.rs
+++ b/crates/data-avalanche-api/src/pages.rs
@@ -235,12 +235,13 @@ pub(crate) fn cross_site(frame: &Frame) -> String {
 const SIGN_OUT: &str = r#"
 <form method="post" action="/ui/sign-out"><button class="btn sec" type="submit">Sign out</button></form>"#;
 
-/// The landing page, until the console's app is built (M15): who is signed in, and, with a
-/// session, a way out.
-pub(crate) fn signed_in(frame: &Frame, name: &str, role: &str, session: bool) -> String {
+/// The page of a binary built without the console's app (`ui/`): who is signed in, how to build
+/// the app, and, with a session, a way out.
+pub(crate) fn not_built(frame: &Frame, name: &str, role: &str, session: bool) -> String {
     let mut card = format!(
         r#"<div class="title">Signed in</div>
-<p>You are <b>{name}</b>, signed in as <b>{role}</b>.</p>"#,
+<p>You are <b>{name}</b>, signed in as <b>{role}</b>.</p>
+<p>The console's app isn't built into this binary: run <code>npm ci &amp;&amp; npm run build</code> in <code>ui/</code>, then build the binary again.</p>"#,
         name = escape(name),
         role = escape(role),
     );
````

`crates/data-avalanche-api/src/server.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/server.rs b/crates/data-avalanche-api/src/server.rs
index b2e0e55..1383a2e 100644
--- a/crates/data-avalanche-api/src/server.rs
+++ b/crates/data-avalanche-api/src/server.rs
@@ -420,6 +420,7 @@ mod tests {
             ready: Arc::new(AtomicBool::new(true)),
             node: Arc::new(OnceLock::new()),
             auth: crate::test_support::open(),
+            app: crate::App::of(&[]),
         };
         tokio::spawn(bound.serve(router(api), std::future::pending()));
 
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index 0802489..4c5b529 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -155,6 +155,7 @@ pub(crate) fn api(auth: Arc<Auth>) -> Api {
         ready: Arc::new(AtomicBool::new(true)),
         node: Arc::default(),
         auth,
+        app: crate::App::of(&[]),
     }
 }
 
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 165a412..3a5c27a 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -8,7 +8,7 @@ use std::sync::{Arc, OnceLock};
 use std::time::{Duration, SystemTime};
 
 use data_avalanche_api::{
-    Api, Auth, Control, Node, Peers, RateLimit, ServeError, Stop, bind, publish, router,
+    Api, App, Auth, Control, Node, Peers, RateLimit, ServeError, Stop, bind, publish, router,
 };
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
@@ -161,6 +161,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
             ready: Arc::clone(&ready),
             node: Arc::clone(&node),
             auth: Arc::clone(&auth),
+            app: App::built(),
         }),
         async {
             let _ = http_stopped.await;
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api --lib console::`
Expected: the API's tests don't compile: `file not found for module app`, and `console.rs` still calls `pages::signed_in`, now `not_built`.

- [ ] **Step 3: Implement**

Write `build.rs` and `app.rs`, and serve them; the E2E session test expects the app's page when the binary carries it:

`crates/data-avalanche-api/build.rs`:

````diff
diff --git a/crates/data-avalanche-api/build.rs b/crates/data-avalanche-api/build.rs
new file mode 100644
index 0000000..893795e
--- /dev/null
+++ b/crates/data-avalanche-api/build.rs
@@ -0,0 +1,60 @@
+//! Embeds the console's app, `ui/dist` as `npm run build` in `ui/` leaves it, into the binary
+//! (Admin_Console §10). Without it the console says its app isn't built, so Rust-only work needs no
+//! Node; `DATA_AVALANCHE_UI=required` (CI, the image) refuses to build without it.
+
+use std::fmt::Write as _;
+use std::path::{Path, PathBuf};
+
+fn main() {
+    let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("cargo sets it"));
+    let ui = manifest.join("../../ui");
+    let dist = ui.join("dist");
+    println!("cargo::rerun-if-env-changed=DATA_AVALANCHE_UI");
+    let mut files = Vec::new();
+    if dist.join("index.html").is_file() {
+        println!("cargo::rerun-if-changed={}", dist.display());
+        walk(&dist, &dist, &mut files);
+    } else {
+        // A path that doesn't exist reruns the script at every build: `ui/` holds `dist` once
+        // it is built.
+        println!("cargo::rerun-if-changed={}", ui.display());
+    }
+    if files.is_empty() && std::env::var("DATA_AVALANCHE_UI").as_deref() == Ok("required") {
+        panic!(
+            "DATA_AVALANCHE_UI=required, but {} holds no index.html: run `npm ci && npm run build` in ui/",
+            dist.display()
+        );
+    }
+    files.sort();
+    let mut out = String::from("&[\n");
+    for (path, file) in &files {
+        writeln!(
+            out,
+            "    AppFile {{ path: {path:?}, body: include_bytes!({:?}) }},",
+            file.display()
+        )
+        .expect("a String takes it");
+    }
+    out.push(']');
+    let target = PathBuf::from(std::env::var_os("OUT_DIR").expect("cargo sets it")).join("app.rs");
+    std::fs::write(target, out).expect("OUT_DIR is writable");
+}
+
+/// Every file under `dir`, as its path from `root` with `/` between names, and where it is.
+fn walk(root: &Path, dir: &Path, files: &mut Vec<(String, PathBuf)>) {
+    for entry in std::fs::read_dir(dir).expect("ui/dist is readable") {
+        let path = entry.expect("ui/dist is readable").path();
+        if path.is_dir() {
+            walk(root, &path, files);
+        } else {
+            let name = path
+                .strip_prefix(root)
+                .expect("under ui/dist")
+                .components()
+                .map(|c| c.as_os_str().to_string_lossy().into_owned())
+                .collect::<Vec<_>>()
+                .join("/");
+            files.push((name, path.canonicalize().expect("a file found exists")));
+        }
+    }
+}
````

`crates/data-avalanche-api/src/app.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/app.rs b/crates/data-avalanche-api/src/app.rs
new file mode 100644
index 0000000..9fdadb3
--- /dev/null
+++ b/crates/data-avalanche-api/src/app.rs
@@ -0,0 +1,86 @@
+//! The console's app (Admin_Console §10): the files `build.rs` embedded from `ui/dist`.
+
+/// A file of the built app: its path under `ui/dist`, and its bytes.
+#[derive(Debug)]
+pub struct AppFile {
+    pub path: &'static str,
+    pub body: &'static [u8],
+}
+
+/// The app built into this binary: empty when `ui/dist` wasn't built.
+static BUILT: &[AppFile] = include!(concat!(env!("OUT_DIR"), "/app.rs"));
+
+/// The console's app: its page (`index.html`) and the files it loads (`app/…`).
+#[derive(Debug, Clone, Copy)]
+pub struct App(&'static [AppFile]);
+
+impl App {
+    /// The app built into this binary.
+    pub fn built() -> Self {
+        Self(BUILT)
+    }
+
+    /// `files` as the app (tests; an empty one is an app not built).
+    pub fn of(files: &'static [AppFile]) -> Self {
+        Self(files)
+    }
+
+    /// The app's page, served for every page under `/ui/`; `None` when it isn't built.
+    pub(crate) fn index(&self) -> Option<&'static [u8]> {
+        self.find("index.html")
+    }
+
+    /// One of the files the page loads, `app/<name>`: its bytes and its content type.
+    pub(crate) fn file(&self, name: &str) -> Option<(&'static [u8], &'static str)> {
+        let path = format!("app/{name}");
+        self.find(&path).map(|body| (body, content_type(&path)))
+    }
+
+    fn find(&self, path: &str) -> Option<&'static [u8]> {
+        self.0.iter().find(|f| f.path == path).map(|f| f.body)
+    }
+}
+
+/// The content type of a built file, by its extension.
+fn content_type(path: &str) -> &'static str {
+    match path.rsplit_once('.').map(|(_, e)| e) {
+        Some("html") => "text/html; charset=utf-8",
+        Some("js") => "text/javascript; charset=utf-8",
+        Some("css") => "text/css; charset=utf-8",
+        Some("svg") => "image/svg+xml",
+        Some("png") => "image/png",
+        Some("woff2") => "font/woff2",
+        Some("json") => "application/json",
+        _ => "application/octet-stream",
+    }
+}
+
+#[cfg(test)]
+mod tests {
+    use super::*;
+
+    #[test]
+    fn a_built_file_s_type_follows_its_extension() {
+        static FILES: &[AppFile] = &[
+            AppFile {
+                path: "app/index-x.css",
+                body: b"",
+            },
+            AppFile {
+                path: "app/logo.svg",
+                body: b"",
+            },
+            AppFile {
+                path: "app/data.bin",
+                body: b"",
+            },
+        ];
+        let app = App::of(FILES);
+        let kind = |name| app.file(name).map(|(_, kind)| kind);
+        assert_eq!(kind("index-x.css"), Some("text/css; charset=utf-8"));
+        assert_eq!(kind("logo.svg"), Some("image/svg+xml"));
+        assert_eq!(kind("data.bin"), Some("application/octet-stream"));
+        assert_eq!(kind("other.css"), None);
+        assert_eq!(app.index(), None);
+    }
+}
````

`crates/data-avalanche-api/src/console.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/console.rs b/crates/data-avalanche-api/src/console.rs
index 159d4e0..7d069b2 100644
--- a/crates/data-avalanche-api/src/console.rs
+++ b/crates/data-avalanche-api/src/console.rs
@@ -59,6 +59,7 @@ pub(crate) fn routes(api: &Api) -> Router<Api> {
         .route("/ui/oauth2/start", get(oauth2::start))
         .route("/ui/oauth2/callback", get(oauth2::callback))
         .route("/ui/assets/{file}", get(asset))
+        .route("/ui/app/{*file}", get(app_file))
         .layer(from_fn_with_state(api.clone(), guard))
 }
 
@@ -226,8 +227,8 @@ pub(crate) fn frame(api: &Api, sign_in: &SignIn) -> Frame {
     }
 }
 
-/// A page under `/ui/` (the app's, from M15): the landing page for a caller who verifies, the
-/// sign-in for one who brought nothing, else why not. An `oauth2` session whose token is about
+/// A page under `/ui/`: the app's page for a caller who verifies, the sign-in for one who brought
+/// nothing, else why not. An `oauth2` session whose token is about
 /// to expire is refreshed first, its new cookies set with the page; once the token expired, a
 /// provider that refuses the refresh ends the session (401), one that can't be reached answers
 /// "try again" (503, `Retry-After`), the session kept.
@@ -274,15 +275,20 @@ async fn page(
         Err(wait) => return too_many(pages::waiting(&frame, &here, retry_secs(wait)), wait),
     };
     let r = match verified {
-        Ok(Verified { principal, session }) => html(
-            StatusCode::OK,
-            pages::signed_in(
-                &frame,
-                &principal.name,
-                &principal.role.to_string(),
-                session.is_some(),
+        Ok(Verified { principal, session }) => match api.app.index() {
+            Some(page) => {
+                ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], page).into_response()
+            }
+            None => html(
+                StatusCode::OK,
+                pages::not_built(
+                    &frame,
+                    &principal.name,
+                    &principal.role.to_string(),
+                    session.is_some(),
+                ),
             ),
-        ),
+        },
         // The provider is down, not the session over: try again in a moment.
         Err(Denied::Unauthenticated { .. })
             if let Some(oauth2::Failed::Unreachable(e)) = &not_refreshed =>
@@ -601,6 +607,22 @@ async fn asset(Path(file): Path<String>) -> Response {
         .into_response()
 }
 
+/// `GET /ui/app/{file}`: a file the app loads, to anyone (code, no data), kept by the browser for
+/// good: a new build names its files anew.
+async fn app_file(State(api): State<Api>, Path(file): Path<String>) -> Response {
+    match api.app.file(&file) {
+        Some((body, kind)) => (
+            [
+                (header::CONTENT_TYPE, kind),
+                (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
+            ],
+            body,
+        )
+            .into_response(),
+        None => StatusCode::NOT_FOUND.into_response(),
+    }
+}
+
 /// The failed basic sign-ins of each address (an IPv6 client by its /64): [`SIGN_IN_ATTEMPTS`]
 /// in a row, and it waits [`SIGN_IN_WAIT`]; a sign-in that succeeds starts the count again. Kept
 /// by each node for itself, in memory, for at most [`ADDRESSES_KEPT`] addresses: past that,
````

`crates/data-avalanche/tests/e2e/console_sessions.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/console_sessions.rs b/crates/data-avalanche/tests/e2e/console_sessions.rs
index 05eb6ab..f4f1e3c 100644
--- a/crates/data-avalanche/tests/e2e/console_sessions.rs
+++ b/crates/data-avalanche/tests/e2e/console_sessions.rs
@@ -25,6 +25,14 @@ fn session_key() -> String {
     URL_SAFE_NO_PAD.encode(b)
 }
 
+/// Whether the binary under test carries the console's app: its build embeds `ui/dist` when
+/// that holds a build (`crates/data-avalanche-api/build.rs`).
+fn app_built() -> bool {
+    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
+        .join("../../ui/dist/index.html")
+        .exists()
+}
+
 /// `n1`, `n2` and `n3` running the test's pipeline, its rows landed, with `globals.api` entries
 /// `api` and the console on, `console` adding to it.
 async fn cluster(test: &str, api: &str, console: &str) -> Cluster {
@@ -81,12 +89,12 @@ async fn every_node_takes(c: &Cluster, cookie: &str, name: &str, role: &str) {
         let p = c.proc(n);
         let (code, _, page) = p.exchange_ok("GET", "/ui/", &with, "").await;
         assert_eq!(code, 200, "{n}: {page}");
-        assert!(
-            page.contains(&format!(
-                "You are <b>{name}</b>, signed in as <b>{role}</b>."
-            )),
-            "{n}: {page}"
-        );
+        let signed_in = if app_built() {
+            r#"<div id="app">"#.to_owned()
+        } else {
+            format!("You are <b>{name}</b>, signed in as <b>{role}</b>.")
+        };
+        assert!(page.contains(&signed_in), "{n}: {page}");
         let (code, _, text) = p.exchange_ok("GET", "/ui/session", &with, "").await;
         assert_eq!(code, 200, "{n}: {text}");
         let v: Value = serde_json::from_str(&text).unwrap();
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-api --lib console::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
(cd ui && npm run check && npm test && npm run build)
```

Expected: no output from the first three; every test passes; svelte-check reports 0 errors and
0 warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/build.rs crates/data-avalanche-api/src/app.rs crates/data-avalanche-api/src/config.rs crates/data-avalanche-api/src/console.rs crates/data-avalanche-api/src/generations.rs crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/pages.rs crates/data-avalanche-api/src/server.rs crates/data-avalanche-api/src/test_support.rs crates/data-avalanche/src/run.rs crates/data-avalanche/tests/e2e/console_sessions.rs
git commit -m "feat(api): the console's app built into the binary and served under /ui/"
```

---

### Task 3: The frame (`ui/src/lib`, `Frame.svelte`, `App.svelte`)

**Files:**
- Modify: `ui/src/App.svelte`
- Create: `ui/src/Frame.svelte`
- Create: `ui/src/Frame.test.ts`
- Create: `ui/src/Later.svelte`
- Modify: `ui/src/app.css`
- Create: `ui/src/lib/api.test.ts`
- Create: `ui/src/lib/api.ts`
- Modify: `ui/src/lib/format.test.ts`
- Modify: `ui/src/lib/format.ts`
- Create: `ui/src/lib/live.svelte.ts`
- Create: `ui/src/lib/live.test.ts`
- Create: `ui/src/lib/router.svelte.ts`
- Create: `ui/src/lib/router.test.ts`
- Create: `ui/src/lib/session.test.ts`
- Create: `ui/src/lib/session.ts`
- Modify: `ui/vite.config.ts`

**Interfaces:**
- Consumes: Task 1's `format.ts`; M14's `GET /ui/session`, `POST /ui/session/refresh`,
  `POST /ui/sign-out`; M13f's `/v1/stream` messages.
- Produces (`session.ts`): `Session { name, role, auth, expires, signed_in, csrf }`,
  `REFRESH_AHEAD_MS` (60 000), `RETRY_MS` (5 000), `readSession(deps): Promise<Session | null>`,
  `keepFresh(first, deps, changed: (s: Session) => void): () => void`.
- Produces (`api.ts`): `READ_WITHIN_MS` (5 000), `HttpError(status, message)`, `Client { get<T>(path),
  send<T>(method, path, body?) }`, `client({ fetch, csrf, ended }): Client`.
- Produces (`live.svelte.ts`): `KINDS`, `STALE_AFTER_MS` (10 000), `OFFLINE_AFTER_MS` (30 000),
  `REOPEN_MS` ([1 000, 2 000, 5 000, 10 000]), `class Live { state, last, on(kind, fn),
  onResync(fn), start(), stop(), retry(), stale(now), tick(now) }`.
- Produces (`router.svelte.ts`): `SECTIONS`, `Section`, `sectionOf(path)`, `isAppLink(a, e)`,
  `class Router { path, section, listen(), go(to) }`.
- Produces: `Frame.svelte` (props `session`, `section`, `generation`, `state`, `last`, `now`,
  `onRetry`, `children`), `Later.svelte`.

- [ ] **Step 1: Write the failing tests**

The tests: `the session` (read, a 401 left to the server; the refresh a minute ahead with the
anti-forgery token; a 503 asked again after `Retry-After`, never a sign-out; a refused refresh
ends it; nothing to refresh without a token); `the calls to /v1`; `the live stream` (by kind;
stale, then offline, a silent stream reopened; a resync after a restart or `lagged`; the
credentials checked once closed; 1, 2, 5, 10 s between tries); `the pages`; `the frame`; and
`clock` to the second. Vitest mounts components, so `vite.config.ts` resolves Svelte's `browser`
condition in test mode.

`ui/src/Frame.test.ts`:

````diff
diff --git a/ui/src/Frame.test.ts b/ui/src/Frame.test.ts
new file mode 100644
index 0000000..53f3672
--- /dev/null
+++ b/ui/src/Frame.test.ts
@@ -0,0 +1,74 @@
+import { flushSync, mount, unmount } from 'svelte';
+import { afterEach, describe, expect, it, vi } from 'vitest';
+import Frame from './Frame.svelte';
+import type { Session } from './lib/session';
+
+const NOW = new Date(2026, 10, 25, 14, 21, 19).getTime();
+
+function session(over: Partial<Session> = {}): Session {
+  return {
+    name: 'Oleksii',
+    role: 'operator',
+    auth: 'basic',
+    expires: null,
+    signed_in: null,
+    csrf: 'anti-forgery',
+    ...over,
+  };
+}
+
+let mounted: ReturnType<typeof mount> | undefined;
+
+function render(props: Partial<Parameters<typeof Frame>[1]> = {}): HTMLElement {
+  const target = document.createElement('div');
+  document.body.replaceChildren(target);
+  mounted = mount(Frame, {
+    target,
+    props: {
+      session: session(),
+      section: 'overview',
+      generation: 42,
+      state: 'live',
+      last: NOW,
+      now: NOW,
+      onRetry: () => {},
+      ...props,
+    },
+  });
+  flushSync();
+  return target;
+}
+
+describe('the frame', () => {
+  afterEach(() => {
+    if (mounted) unmount(mounted);
+  });
+
+  it('names the sections, the one shown marked, and who is signed in', () => {
+    const page = render({ section: 'pipelines' });
+    const nav = [...page.querySelectorAll('nav a')].map((a) => a.textContent);
+    expect(nav).toEqual(['Overview', 'Cluster', 'Pipelines', 'Tables', 'Snapshots', 'Events', 'Config']);
+    expect(page.querySelector('[aria-current="page"]')?.getAttribute('href')).toBe('/ui/pipelines');
+    expect(page.querySelector('.who')?.textContent).toMatch(/live\s+config gen 42\s+Oleksii\s+operator/);
+    expect(page.querySelector('form[action="/ui/sign-out"]')).not.toBeNull();
+  });
+
+  it("offers no sign-out without a session, and warns when there's no authentication", () => {
+    expect(render({ session: session({ auth: 'jwt', csrf: null }) }).querySelector('form')).toBeNull();
+    const open = render({ session: session({ auth: 'none', csrf: null, role: 'admin' }) });
+    expect(open.querySelector('[role="alert"]')?.textContent).toContain(
+      'This cluster has no authentication: everyone who reaches the console is admin.',
+    );
+  });
+
+  it('says how old the page is once the updates stop', () => {
+    const late = render({ state: 'reconnecting', now: NOW + 14_000 });
+    expect(late.querySelector('[role="status"]')?.textContent).toBe('reconnecting · last update 14 s ago');
+    const onRetry = vi.fn();
+    const offline = render({ state: 'offline', now: NOW + 60_000, onRetry });
+    const alert = offline.querySelector('.down');
+    expect(alert?.textContent).toContain('Offline since 14:21:19 — everything below is as of then.');
+    alert?.querySelector('button')?.click();
+    expect(onRetry).toHaveBeenCalledOnce();
+  });
+});
````

`ui/src/lib/api.test.ts`:

````diff
diff --git a/ui/src/lib/api.test.ts b/ui/src/lib/api.test.ts
new file mode 100644
index 0000000..95dabf2
--- /dev/null
+++ b/ui/src/lib/api.test.ts
@@ -0,0 +1,49 @@
+import { describe, expect, it, vi } from 'vitest';
+import { client, HttpError } from './api';
+
+const json = (status: number, body: unknown) => new Response(JSON.stringify(body), { status });
+
+describe('the calls to /v1', () => {
+  it('read JSON within their time limit', async () => {
+    const fetch = vi.fn().mockResolvedValue(json(200, { pipelines: [] }));
+    const api = client({ fetch, csrf: () => 'token', ended: vi.fn() });
+    expect(await api.get('/v1/pipelines')).toEqual({ pipelines: [] });
+    const [path, init] = fetch.mock.calls[0];
+    expect(path).toBe('/v1/pipelines');
+    expect(init.signal).toBeInstanceOf(AbortSignal);
+    expect(init.headers).toEqual({ Accept: 'application/json' });
+  });
+
+  it('send the anti-forgery token with what changes something', async () => {
+    const fetch = vi.fn().mockImplementation(async () => json(202, { accepted: true }));
+    const api = client({ fetch, csrf: () => 'token', ended: vi.fn() });
+    await api.send('POST', '/v1/pipelines/orders/pause', { note: 'upgrade' });
+    expect(fetch).toHaveBeenCalledWith('/v1/pipelines/orders/pause', {
+      method: 'POST',
+      headers: {
+        Accept: 'application/json',
+        'Content-Type': 'application/json',
+        'X-CSRF-Token': 'token',
+      },
+      body: '{"note":"upgrade"}',
+    });
+    // Without a session (none, jwt): no token.
+    const bare = client({ fetch, csrf: () => null, ended: vi.fn() });
+    await bare.send('POST', '/v1/pipelines/orders/flush');
+    expect(fetch.mock.calls[1][1].headers).not.toHaveProperty('X-CSRF-Token');
+  });
+
+  it("leave the page to the server on a 401, and say a refusal's reason", async () => {
+    const ended = vi.fn();
+    const fetch = vi
+      .fn()
+      .mockResolvedValueOnce(json(401, { error: 'the session ended' }))
+      .mockResolvedValueOnce(json(403, { error: 'operator or above' }));
+    const api = client({ fetch, csrf: () => null, ended });
+    await expect(api.get('/v1/pipelines')).rejects.toEqual(new HttpError(401, 'the session ended'));
+    expect(ended).toHaveBeenCalledOnce();
+    const refused = api.send('POST', '/v1/pipelines/orders/pause');
+    await expect(refused).rejects.toMatchObject({ status: 403, message: 'operator or above' });
+    expect(ended).toHaveBeenCalledOnce();
+  });
+});
````

`ui/src/lib/format.test.ts`:

````diff
diff --git a/ui/src/lib/format.test.ts b/ui/src/lib/format.test.ts
index b6372e5..4ce7363 100644
--- a/ui/src/lib/format.test.ts
+++ b/ui/src/lib/format.test.ts
@@ -27,8 +27,9 @@ describe('the console writes', () => {
     expect(bytes(1536)).toBe('1.5 kB');
   });
 
-  it('the time of day, hours and minutes', () => {
+  it('the time of day, to the minute or the second', () => {
     const at = new Date(2026, 10, 25, 9, 5, 30).getTime();
     expect(clock(at)).toBe('09:05');
+    expect(clock(at, true)).toBe('09:05:30');
   });
 });
````

`ui/src/lib/live.test.ts`:

````diff
diff --git a/ui/src/lib/live.test.ts b/ui/src/lib/live.test.ts
new file mode 100644
index 0000000..bb084a1
--- /dev/null
+++ b/ui/src/lib/live.test.ts
@@ -0,0 +1,123 @@
+import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
+import { Live, OFFLINE_AFTER_MS, STALE_AFTER_MS, type SourceLike } from './live.svelte';
+
+/** An `EventSource` the test drives. */
+class FakeSource implements SourceLike {
+  readyState = 0;
+  onerror: ((e: Event) => void) | null = null;
+  closed = false;
+  #fns = new Map<string, (e: MessageEvent) => void>();
+  constructor(readonly url: string) {}
+  addEventListener(kind: string, fn: (e: MessageEvent) => void) {
+    this.#fns.set(kind, fn);
+  }
+  close() {
+    this.closed = true;
+    this.readyState = 2;
+  }
+  send(kind: string, data: unknown) {
+    this.readyState = 1;
+    this.#fns.get(kind)?.(new MessageEvent(kind, { data: JSON.stringify(data) }));
+  }
+  fail() {
+    this.readyState = 2;
+    this.onerror?.(new Event('error'));
+  }
+}
+
+const NOW = Date.UTC(2026, 10, 25, 14, 0, 0);
+
+describe('the live stream', () => {
+  let sources: FakeSource[];
+  const open = (url: string) => {
+    const s = new FakeSource(url);
+    sources.push(s);
+    return s;
+  };
+
+  beforeEach(() => {
+    sources = [];
+    vi.useFakeTimers();
+    vi.setSystemTime(NOW);
+  });
+  afterEach(() => vi.useRealTimers());
+
+  it("hands each message to its kind's listeners, and is live while they come", () => {
+    const live = new Live({ open, check: vi.fn() });
+    const states = vi.fn();
+    live.on('pipeline', states);
+    live.start();
+    expect(sources[0].url).toBe('/v1/stream');
+    expect(live.state).toBe('connecting');
+    sources[0].send('hello', { node: 'n1', follows: ['n2'] });
+    sources[0].send('pipeline', { pipeline: 'orders', actual: 'paused', node: 'n2' });
+    expect(states).toHaveBeenCalledWith({ pipeline: 'orders', actual: 'paused', node: 'n2' });
+    expect(live.state).toBe('live');
+    expect(live.last).toBe(NOW);
+  });
+
+  it('turns stale, then offline, as the messages stop, and reopens a silent stream', async () => {
+    const live = new Live({ open, check: vi.fn().mockResolvedValue('ok') });
+    live.start();
+    sources[0].send('hello', { node: 'n1' });
+    vi.setSystemTime(NOW + STALE_AFTER_MS - 1);
+    live.tick(Date.now());
+    expect([live.state, live.stale(Date.now())]).toEqual(['live', false]);
+    vi.setSystemTime(NOW + STALE_AFTER_MS);
+    live.tick(Date.now());
+    expect([live.state, live.stale(Date.now())]).toEqual(['reconnecting', true]);
+    vi.setSystemTime(NOW + OFFLINE_AFTER_MS);
+    live.tick(Date.now());
+    expect(live.state).toBe('offline');
+    expect(sources[0].closed).toBe(true);
+    await vi.advanceTimersByTimeAsync(1_000);
+    expect(sources).toHaveLength(2);
+  });
+
+  it('reads the state again after it restarts or misses messages: no replay', () => {
+    const live = new Live({ open, check: vi.fn() });
+    const resync = vi.fn();
+    live.onResync(resync);
+    live.start();
+    sources[0].send('hello', { node: 'n1' });
+    expect(resync).not.toHaveBeenCalled();
+    sources[0].send('reset', { node: 'n1', reason: 'lagged', missed: 12 });
+    expect(resync).toHaveBeenCalledTimes(1);
+    // The browser reconnected by itself: a new hello.
+    sources[0].send('hello', { node: 'n2' });
+    expect(resync).toHaveBeenCalledTimes(2);
+  });
+
+  it("checks the credentials once it closes, and opens again only if they hold", async () => {
+    const check = vi.fn().mockResolvedValueOnce('ok').mockResolvedValueOnce('ended');
+    const live = new Live({ open, check });
+    live.start();
+    sources[0].send('hello', { node: 'n1' });
+    sources[0].send('reset', { node: 'n1', reason: 'credentials' });
+    expect(sources[0].closed).toBe(true);
+    await vi.advanceTimersByTimeAsync(1_000);
+    expect(check).toHaveBeenCalledOnce();
+    expect(sources).toHaveLength(2);
+    // Refused now (a 401): the page was left to the server. No message came since the last
+    // try, so the next waits longer.
+    sources[1].fail();
+    await vi.advanceTimersByTimeAsync(2_000);
+    expect(check).toHaveBeenCalledTimes(2);
+    expect(sources).toHaveLength(2);
+  });
+
+  it('waits 1, 2, 5, then 10 s between tries, and retries at once when asked', async () => {
+    const live = new Live({ open, check: vi.fn().mockResolvedValue('later') });
+    live.start();
+    for (const wait of [1_000, 2_000, 5_000, 10_000, 10_000]) {
+      const n = sources.length;
+      sources[n - 1].fail();
+      await vi.advanceTimersByTimeAsync(wait - 1);
+      expect(sources).toHaveLength(n);
+      await vi.advanceTimersByTimeAsync(1);
+      expect(sources).toHaveLength(n + 1);
+    }
+    live.retry();
+    expect(sources).toHaveLength(7);
+  });
+});
````

`ui/src/lib/router.test.ts`:

````diff
diff --git a/ui/src/lib/router.test.ts b/ui/src/lib/router.test.ts
new file mode 100644
index 0000000..d76a7cc
--- /dev/null
+++ b/ui/src/lib/router.test.ts
@@ -0,0 +1,28 @@
+import { describe, expect, it } from 'vitest';
+import { isAppLink, sectionOf } from './router.svelte';
+
+describe('the pages', () => {
+  it('are named by their first name under /ui/', () => {
+    expect(sectionOf('/ui/')).toBe('overview');
+    expect(sectionOf('/ui/pipelines')).toBe('pipelines');
+    expect(sectionOf('/ui/pipelines/orders?tab=tables')).toBe('pipelines');
+    expect(sectionOf('/ui/nowhere')).toBe('overview');
+  });
+
+  it("follow a plain click on the app's own links only", () => {
+    const link = (href: string, attrs: Record<string, string> = {}) => {
+      const a = document.createElement('a');
+      a.href = href;
+      for (const [k, v] of Object.entries(attrs)) a.setAttribute(k, v);
+      return a;
+    };
+    const click = (init: MouseEventInit = {}) => new MouseEvent('click', { button: 0, ...init });
+    expect(isAppLink(link('/ui/pipelines'), click())).toBe(true);
+    expect(isAppLink(link('/ui/pipelines'), click({ metaKey: true }))).toBe(false);
+    expect(isAppLink(link('/ui/pipelines', { target: '_blank' }), click())).toBe(false);
+    expect(isAppLink(link('/ui/sign-in?next=%2Fui%2F'), click())).toBe(false);
+    expect(isAppLink(link('/ui/app/index-x.js'), click())).toBe(false);
+    expect(isAppLink(link('/v1/openapi.json'), click())).toBe(false);
+    expect(isAppLink(link('https://example.com/ui/'), click())).toBe(false);
+  });
+});
````

`ui/src/lib/session.test.ts`:

````diff
diff --git a/ui/src/lib/session.test.ts b/ui/src/lib/session.test.ts
new file mode 100644
index 0000000..fbe82f3
--- /dev/null
+++ b/ui/src/lib/session.test.ts
@@ -0,0 +1,88 @@
+import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
+import { keepFresh, readSession, REFRESH_AHEAD_MS, type Session } from './session';
+
+const NOW = Date.UTC(2026, 10, 25, 14, 0, 0);
+
+function session(over: Partial<Session> = {}): Session {
+  return {
+    name: 'ops',
+    role: 'operator',
+    auth: 'oauth2',
+    expires: new Date(NOW + 10 * 60_000).toISOString(),
+    signed_in: new Date(NOW).toISOString(),
+    csrf: 'anti-forgery',
+    ...over,
+  };
+}
+
+function answer(status: number, body: unknown = {}, headers: Record<string, string> = {}) {
+  return new Response(JSON.stringify(body), { status, headers });
+}
+
+describe('the session', () => {
+  beforeEach(() => {
+    vi.useFakeTimers();
+    vi.setSystemTime(NOW);
+  });
+  afterEach(() => vi.useRealTimers());
+
+  it('is read from the server, and a 401 leaves the page to it', async () => {
+    const ended = vi.fn();
+    const ok = vi.fn().mockResolvedValue(answer(200, session()));
+    expect((await readSession({ fetch: ok, ended }))?.name).toBe('ops');
+    const gone = vi.fn().mockResolvedValue(answer(401, { error: 'expired' }));
+    expect(await readSession({ fetch: gone, ended })).toBeNull();
+    expect(ended).toHaveBeenCalledOnce();
+  });
+
+  it("refreshes an oauth2 token a minute before it expires, with the anti-forgery token", async () => {
+    const renewed = session({ expires: new Date(NOW + 20 * 60_000).toISOString() });
+    const fetch = vi.fn().mockImplementation(async () => answer(200, renewed));
+    const changed = vi.fn();
+    keepFresh(session(), { fetch, ended: vi.fn() }, changed);
+    await vi.advanceTimersByTimeAsync(10 * 60_000 - REFRESH_AHEAD_MS - 1);
+    expect(fetch).not.toHaveBeenCalled();
+    await vi.advanceTimersByTimeAsync(1);
+    expect(fetch).toHaveBeenCalledWith('/ui/session/refresh', {
+      method: 'POST',
+      headers: { Accept: 'application/json', 'X-CSRF-Token': 'anti-forgery' },
+    });
+    expect(changed).toHaveBeenCalledWith(renewed);
+    // The next one, from the new expiry.
+    await vi.advanceTimersByTimeAsync(10 * 60_000);
+    expect(fetch).toHaveBeenCalledTimes(2);
+  });
+
+  it('asks again after Retry-After when the provider is down, and never signs out for it', async () => {
+    const fetch = vi
+      .fn()
+      .mockResolvedValueOnce(answer(503, { error: 'down' }, { 'Retry-After': '5' }))
+      .mockResolvedValueOnce(answer(200, session({ expires: null })));
+    const ended = vi.fn();
+    keepFresh(session({ expires: new Date(NOW + 30_000).toISOString() }), { fetch, ended }, vi.fn());
+    await vi.advanceTimersByTimeAsync(0);
+    expect(fetch).toHaveBeenCalledTimes(1);
+    await vi.advanceTimersByTimeAsync(4_999);
+    expect(fetch).toHaveBeenCalledTimes(1);
+    await vi.advanceTimersByTimeAsync(1);
+    expect(fetch).toHaveBeenCalledTimes(2);
+    expect(ended).not.toHaveBeenCalled();
+  });
+
+  it('ends when the provider refuses the refresh', async () => {
+    const fetch = vi.fn().mockResolvedValue(answer(401, { error: 'invalid_grant' }));
+    const ended = vi.fn();
+    keepFresh(session({ expires: new Date(NOW).toISOString() }), { fetch, ended }, vi.fn());
+    await vi.advanceTimersByTimeAsync(0);
+    expect(ended).toHaveBeenCalledOnce();
+  });
+
+  it('leaves alone what has no token to refresh', async () => {
+    const fetch = vi.fn();
+    for (const s of [session({ auth: 'basic' }), session({ expires: null }), session({ auth: 'jwt' })]) {
+      keepFresh(s, { fetch, ended: vi.fn() }, vi.fn());
+    }
+    await vi.advanceTimersByTimeAsync(24 * 3600_000);
+    expect(fetch).not.toHaveBeenCalled();
+  });
+});
````

`ui/vite.config.ts`:

````diff
diff --git a/ui/vite.config.ts b/ui/vite.config.ts
index c0c6ca3..18a893c 100644
--- a/ui/vite.config.ts
+++ b/ui/vite.config.ts
@@ -3,9 +3,11 @@ import { defineConfig } from 'vitest/config';
 
 // The node serves the app under /ui/ (Admin_Console §10), its built files under /ui/app/: the
 // server's own pages keep /ui/assets/.
-export default defineConfig({
+export default defineConfig(({ mode }) => ({
   base: '/ui/',
   plugins: [svelte()],
   build: { assetsDir: 'app', target: 'es2022' },
+  // The tests mount components in jsdom: Svelte's browser build.
+  resolve: mode === 'test' ? { conditions: ['browser'] } : undefined,
   test: { environment: 'jsdom', include: ['src/**/*.test.ts'] },
-});
+}));
````

- [ ] **Step 2: Run them to see them fail**

Run: `cd ui && npm test`
Expected: six test files fail: `Failed to resolve import` for `./api`, `./session`, `./live.svelte`, `./router.svelte` and `./Frame.svelte`, and `the time of day, to the minute or the second` fails (`clock` takes no seconds yet); the other 3 tests pass.

- [ ] **Step 3: Implement**

Write the session, the client, the stream, the router, the frame and the app's wiring:

`ui/src/App.svelte`:

````diff
diff --git a/ui/src/App.svelte b/ui/src/App.svelte
index 60f3d3a..0babb3d 100644
--- a/ui/src/App.svelte
+++ b/ui/src/App.svelte
@@ -1,6 +1,79 @@
 <script lang="ts">
+  // The console's app: who is signed in, the live stream, and the page the address names.
+  import { onMount } from 'svelte';
+  import Frame from './Frame.svelte';
+  import Later from './Later.svelte';
+  import { client } from './lib/api';
+  import { Live } from './lib/live.svelte';
+  import { Router } from './lib/router.svelte';
+  import { keepFresh, readSession, type Session } from './lib/session';
+
+  // The server says why the session ended: its 401 page, or the sign-in.
+  const ended = () => location.reload();
+  const fetch = globalThis.fetch.bind(globalThis);
+
+  let session = $state<Session | null>(null);
+  let generation = $state<number | null>(null);
+  let now = $state(Date.now());
+  const router = new Router();
+  const api = client({ fetch, csrf: () => session?.csrf ?? null, ended });
+  const live = new Live({
+    open: (url) => new EventSource(url),
+    check: async () => {
+      const r = await fetch('/ui/session', { headers: { Accept: 'application/json' } });
+      if (r.status === 401) {
+        ended();
+        return 'ended';
+      }
+      return r.ok ? 'ok' : 'later';
+    },
+  });
+
+  async function readGeneration() {
+    try {
+      generation = (await api.get<{ generation: number }>('/v1/config/status')).generation;
+    } catch {
+      // The generation stays as last read; the page shows how old the rest is.
+    }
+  }
+
+  onMount(() => {
+    const stops = [
+      router.listen(),
+      live.onResync(() => void readGeneration()),
+      live.on('event', (e) => {
+        if ((e as { kind?: string }).kind === 'config.published') void readGeneration();
+      }),
+    ];
+    void readSession({ fetch, ended }).then((s) => {
+      if (s === null) return;
+      session = s;
+      stops.push(keepFresh(s, { fetch, ended }, (next) => (session = next)));
+      live.start();
+      void readGeneration();
+    });
+    const tick = setInterval(() => {
+      now = Date.now();
+      live.tick(now);
+    }, 1000);
+    return () => {
+      for (const stop of stops) stop();
+      live.stop();
+      clearInterval(tick);
+    };
+  });
 </script>
 
-<main>
-  <h1>data-avalanche</h1>
-</main>
+{#if session !== null}
+  <Frame
+    {session}
+    section={router.section}
+    {generation}
+    state={live.state}
+    last={live.last}
+    {now}
+    onRetry={() => live.retry()}
+  >
+    <Later section={router.section} />
+  </Frame>
+{/if}
````

`ui/src/Frame.svelte`:

````diff
diff --git a/ui/src/Frame.svelte b/ui/src/Frame.svelte
new file mode 100644
index 0000000..f2c6072
--- /dev/null
+++ b/ui/src/Frame.svelte
@@ -0,0 +1,133 @@
+<script lang="ts">
+  // The top bar every page shares (Admin_Console §4; mockups 01 and 11): the sections, whether
+  // the page is live, the config generation, and who is signed in with their role.
+  import type { Snippet } from 'svelte';
+  import { clock } from './lib/format';
+  import type { State } from './lib/live.svelte';
+  import { SECTIONS, type Section } from './lib/router.svelte';
+  import type { Session } from './lib/session';
+
+  interface Props {
+    session: Session;
+    section: Section;
+    generation: number | null;
+    state: State;
+    /** When the last message came (ms); `null` before the first. */
+    last: number | null;
+    now: number;
+    onRetry: () => void;
+    children?: Snippet;
+  }
+  let { session, section, generation, state, last, now, onRetry, children }: Props = $props();
+
+  const connection = $derived.by(() => {
+    if (state === 'reconnecting' && last !== null) {
+      return `reconnecting · last update ${Math.floor((now - last) / 1000)} s ago`;
+    }
+    return state;
+  });
+</script>
+
+<header class="bar">
+  <a class="brand" href="/ui/">data-avalanche</a>
+  <nav aria-label="Sections">
+    {#each SECTIONS as s (s.id)}
+      <a href={s.path} aria-current={s.id === section ? 'page' : undefined}>{s.label}</a>
+    {/each}
+  </nav>
+  <div class="who">
+    <span class="conn {state}" role="status">{connection}</span>
+    {#if generation !== null}<span>config gen {generation}</span>{/if}
+    <span>{session.name}</span>
+    <b>{session.role}</b>
+    {#if session.csrf !== null}
+      <form method="post" action="/ui/sign-out"><button type="submit">Sign out</button></form>
+    {/if}
+  </div>
+</header>
+{#if session.auth === 'none'}
+  <div class="banner" role="alert">
+    This cluster has no authentication: everyone who reaches the console is admin.
+  </div>
+{/if}
+{#if state === 'offline'}
+  <div class="down" role="alert">
+    {#if last !== null}Offline since {clock(last, true)} — everything below is as of then.{:else}Offline:
+      the console can't reach the cluster's live updates.{/if}
+    <button type="button" onclick={onRetry}>Retry now</button>
+  </div>
+{/if}
+<main>{@render children?.()}</main>
+
+<style>
+  .bar {
+    display: flex;
+    align-items: center;
+    gap: 18px;
+    padding: 10px 16px;
+    background: var(--bg-bar);
+    border-bottom: 1px solid var(--border);
+  }
+  .brand {
+    font-weight: 600;
+    font-size: 14px;
+    color: var(--text);
+    text-decoration: none;
+  }
+  nav a {
+    margin-right: 14px;
+    color: var(--text-2);
+    text-decoration: none;
+  }
+  nav a[aria-current='page'] {
+    color: var(--text);
+    font-weight: 600;
+    border-bottom: 2px solid var(--accent);
+    padding-bottom: 2px;
+  }
+  .who {
+    margin-left: auto;
+    display: flex;
+    align-items: center;
+    gap: 8px;
+    color: var(--text-2);
+  }
+  .who b {
+    color: var(--text);
+  }
+  .conn::before {
+    content: '';
+    display: inline-block;
+    width: 7px;
+    height: 7px;
+    border-radius: 50%;
+    margin-right: 5px;
+    background: var(--grey);
+  }
+  .conn.live::before {
+    background: var(--green);
+  }
+  .conn.reconnecting::before {
+    background: var(--orange);
+  }
+  .conn.offline::before {
+    background: var(--red);
+  }
+  form {
+    display: inline;
+  }
+  .banner,
+  .down {
+    padding: 8px 16px;
+    border-bottom: 1px solid var(--border);
+  }
+  .banner {
+    background: var(--orange-bg);
+  }
+  .down {
+    background: var(--red-bg);
+  }
+  main {
+    padding: 16px;
+  }
+</style>
````

`ui/src/Later.svelte`:

````diff
diff --git a/ui/src/Later.svelte b/ui/src/Later.svelte
new file mode 100644
index 0000000..d390ac6
--- /dev/null
+++ b/ui/src/Later.svelte
@@ -0,0 +1,10 @@
+<script lang="ts">
+  // A section whose page isn't built yet (M16 builds them).
+  import { SECTIONS, type Section } from './lib/router.svelte';
+
+  let { section }: { section: Section } = $props();
+  const label = $derived(SECTIONS.find((s) => s.id === section)?.label ?? '');
+</script>
+
+<h2>{label}</h2>
+<p class="muted">This page isn't in this build yet.</p>
````

`ui/src/app.css`:

````diff
diff --git a/ui/src/app.css b/ui/src/app.css
index a8bc41a..b2a2c35 100644
--- a/ui/src/app.css
+++ b/ui/src/app.css
@@ -1,3 +1,73 @@
+/* The console's colours, light and dark as the system says (Admin_Console §10). A state is
+   never told by colour alone: each carries its label. */
 :root {
+  --bg: #f5f5f7;
+  --bg-panel: #ffffff;
+  --bg-bar: #e9e9ec;
+  --border: #d1d1d6;
+  --text: #1d1d1f;
+  --text-2: #6e6e73;
+  --accent: #0066cc;
+  --green: #1f8f3a;
+  --orange: #b86e00;
+  --yellow: #a68b00;
+  --red: #d0302a;
+  --grey: #8e8e93;
+  --green-bg: rgba(52, 199, 89, 0.16);
+  --orange-bg: rgba(255, 159, 10, 0.18);
+  --yellow-bg: rgba(230, 194, 0, 0.18);
+  --red-bg: rgba(255, 59, 48, 0.14);
+  --grey-bg: rgba(142, 142, 147, 0.18);
+  color-scheme: light dark;
   font: 13px/1.45 system-ui, -apple-system, 'Segoe UI', sans-serif;
 }
+
+@media (prefers-color-scheme: dark) {
+  :root {
+    --bg: #1d1d1f;
+    --bg-panel: #2a2a2c;
+    --bg-bar: #333336;
+    --border: #48484a;
+    --text: #f5f5f7;
+    --text-2: #a1a1a6;
+    --accent: #4da3ff;
+    --green: #4cd964;
+    --orange: #ffb340;
+    --yellow: #ffd60a;
+    --red: #ff6961;
+    --grey: #98989d;
+  }
+}
+
+* {
+  box-sizing: border-box;
+}
+
+body {
+  margin: 0;
+  background: var(--bg);
+  color: var(--text);
+}
+
+a {
+  color: var(--accent);
+}
+
+h2 {
+  font-size: 18px;
+  margin: 0 0 12px;
+}
+
+button {
+  font: inherit;
+  padding: 3px 10px;
+  border: 1px solid var(--border);
+  border-radius: 6px;
+  background: var(--bg-panel);
+  color: var(--text);
+  cursor: pointer;
+}
+
+.muted {
+  color: var(--text-2);
+}
````

`ui/src/lib/api.ts`:

````diff
diff --git a/ui/src/lib/api.ts b/ui/src/lib/api.ts
new file mode 100644
index 0000000..fb104c1
--- /dev/null
+++ b/ui/src/lib/api.ts
@@ -0,0 +1,62 @@
+// The console's calls to `/v1` (Admin_Console §9.1): the session's cookie goes by itself; a call
+// that changes something adds the session's anti-forgery token. A 401 leaves the page to the
+// server, which says why the session ended.
+
+/** How long one section of a page waits for its read (Admin_Console §8: each has its own). */
+export const READ_WITHIN_MS = 5_000;
+
+/** A `/v1` answer that isn't a success: its status and the server's `error`. */
+export class HttpError extends Error {
+  constructor(
+    readonly status: number,
+    message: string,
+  ) {
+    super(message);
+  }
+}
+
+export interface Deps {
+  fetch: typeof fetch;
+  /** The session's anti-forgery token, `null` without a session. */
+  csrf: () => string | null;
+  ended: () => void;
+}
+
+export interface Client {
+  /** `GET path`, within `READ_WITHIN_MS`. */
+  get<T>(path: string): Promise<T>;
+  /** A call that changes something, with its JSON body. */
+  send<T>(method: 'POST' | 'PUT' | 'DELETE', path: string, body?: unknown): Promise<T>;
+}
+
+export function client(deps: Deps): Client {
+  const call = async <T>(path: string, init: RequestInit): Promise<T> => {
+    const r = await deps.fetch(path, init);
+    if (r.status === 401) deps.ended();
+    if (!r.ok) {
+      const body = (await r.json().catch(() => ({}))) as { error?: string };
+      throw new HttpError(r.status, body.error ?? r.statusText);
+    }
+    return (await r.json()) as T;
+  };
+  return {
+    get: (path) =>
+      call(path, {
+        headers: { Accept: 'application/json' },
+        signal: AbortSignal.timeout(READ_WITHIN_MS),
+      }),
+    send: (method, path, body) => {
+      const headers: Record<string, string> = {
+        Accept: 'application/json',
+        'Content-Type': 'application/json',
+      };
+      const csrf = deps.csrf();
+      if (csrf !== null) headers['X-CSRF-Token'] = csrf;
+      return call(path, {
+        method,
+        headers,
+        body: body === undefined ? undefined : JSON.stringify(body),
+      });
+    },
+  };
+}
````

`ui/src/lib/format.ts`:

````diff
diff --git a/ui/src/lib/format.ts b/ui/src/lib/format.ts
index 545e408..58c0aa0 100644
--- a/ui/src/lib/format.ts
+++ b/ui/src/lib/format.ts
@@ -32,8 +32,10 @@ export function bytes(n: number): string {
   return `${v < 10 ? v.toFixed(1).replace(/\.0$/, '') : Math.round(v)} ${UNITS[unit]}`;
 }
 
-/** The local time of day of `at` (ms): `09:05`. */
-export function clock(at: number): string {
+/** The local time of day of `at` (ms): `09:05`, or `09:05:30` with `seconds`. */
+export function clock(at: number, seconds = false): string {
   const d = new Date(at);
-  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
+  const two = (n: number) => String(n).padStart(2, '0');
+  const hm = `${two(d.getHours())}:${two(d.getMinutes())}`;
+  return seconds ? `${hm}:${two(d.getSeconds())}` : hm;
 }
````

`ui/src/lib/live.svelte.ts`:

````diff
diff --git a/ui/src/lib/live.svelte.ts b/ui/src/lib/live.svelte.ts
new file mode 100644
index 0000000..7136849
--- /dev/null
+++ b/ui/src/lib/live.svelte.ts
@@ -0,0 +1,172 @@
+// The live stream (Admin_Console §8, §8.1): one `EventSource` on `/v1/stream` per page. Each
+// message is handed to whoever listens for its kind; the page reads the current state when the
+// stream (re)starts or missed some, since the stream has no replay. How old the last message
+// is says whether the page is live, stale or offline: a number is never shown as current when it
+// isn't.
+
+export const KINDS = [
+  'hello',
+  'pipeline',
+  'figures',
+  'node',
+  'samples',
+  'event',
+  'members',
+  'peer',
+  'reset',
+] as const;
+export type Kind = (typeof KINDS)[number];
+
+/** After this long without a message every panel is stale: each node sends every 5 s. */
+export const STALE_AFTER_MS = 10_000;
+/** After this long without a message the page is offline: everything on it is as of then. */
+export const OFFLINE_AFTER_MS = 30_000;
+/** The waits before opening the stream again: 1, 2, 5, then every 10 s. */
+export const REOPEN_MS = [1_000, 2_000, 5_000, 10_000];
+
+export type State = 'connecting' | 'live' | 'reconnecting' | 'offline';
+
+/** What the stream needs of an `EventSource`. */
+export interface SourceLike {
+  readonly readyState: number;
+  onerror: ((e: Event) => void) | null;
+  addEventListener(kind: string, fn: (e: MessageEvent) => void): void;
+  close(): void;
+}
+
+export interface Deps {
+  open: (url: string) => SourceLike;
+  /**
+   * Once the stream closed, whether its credentials still hold: go on (`ok`), the page was left
+   * to the server (`ended`), or ask again later.
+   */
+  check: () => Promise<'ok' | 'ended' | 'later'>;
+}
+
+const CLOSED = 2;
+
+export class Live {
+  state = $state<State>('connecting');
+  /** When the last message came (ms since the epoch); `null` before the first. */
+  last = $state<number | null>(null);
+
+  #deps: Deps;
+  #url: string;
+  #source: SourceLike | null = null;
+  #listeners = new Map<Kind, Set<(data: unknown) => void>>();
+  #resync = new Set<() => void>();
+  #hellos = 0;
+  #opened = 0;
+  #reopens = 0;
+  #timer: ReturnType<typeof setTimeout> | undefined;
+  #stopped = false;
+
+  constructor(deps: Deps, url = '/v1/stream') {
+    this.#deps = deps;
+    this.#url = url;
+  }
+
+  /** Hands each message of `kind` to `fn`; returns the way to stop. */
+  on(kind: Kind, fn: (data: unknown) => void): () => void {
+    const set = this.#listeners.get(kind) ?? new Set();
+    set.add(fn);
+    this.#listeners.set(kind, set);
+    return () => set.delete(fn);
+  }
+
+  /** `fn` reads the current state again: the stream restarted, or missed messages. */
+  onResync(fn: () => void): () => void {
+    this.#resync.add(fn);
+    return () => this.#resync.delete(fn);
+  }
+
+  start(): void {
+    this.#stopped = false;
+    this.#open();
+  }
+
+  stop(): void {
+    this.#stopped = true;
+    clearTimeout(this.#timer);
+    this.#source?.close();
+    this.#source = null;
+  }
+
+  /** Opens the stream again now ("Retry now"). */
+  retry(): void {
+    clearTimeout(this.#timer);
+    this.#source?.close();
+    this.#reopens = 0;
+    this.#open();
+  }
+
+  /** Whether the page's numbers are older than `STALE_AFTER_MS` at `now`. */
+  stale(now: number): boolean {
+    return now - (this.last ?? this.#opened) >= STALE_AFTER_MS;
+  }
+
+  /** Brings `state` up to `now`; the page calls it every second. A stream silent past
+   *  `OFFLINE_AFTER_MS` is opened again: a proxy may hold a dead connection. */
+  tick(now: number): void {
+    const quiet = now - (this.last ?? this.#opened);
+    if (quiet < STALE_AFTER_MS) {
+      this.state = this.last === null ? 'connecting' : 'live';
+    } else if (quiet < OFFLINE_AFTER_MS) {
+      this.state = 'reconnecting';
+    } else {
+      this.state = 'offline';
+      if (this.#source !== null && this.#timer === undefined) this.#closed();
+    }
+  }
+
+  #open(): void {
+    this.#timer = undefined;
+    if (this.#stopped) return;
+    this.#opened ||= Date.now();
+    const source = this.#deps.open(this.#url);
+    this.#source = source;
+    for (const kind of KINDS) {
+      source.addEventListener(kind, (e) => this.#message(kind, e.data as string));
+    }
+    source.onerror = () => {
+      // Still connecting: the browser tries again by itself. Closed: a refusal (a 401), or
+      // a reset; see whether the credentials still hold before opening it again.
+      if (source.readyState === CLOSED && this.#source === source) this.#closed();
+    };
+  }
+
+  #message(kind: Kind, text: string): void {
+    let data: unknown;
+    try {
+      data = JSON.parse(text);
+    } catch {
+      return;
+    }
+    this.last = Date.now();
+    this.#reopens = 0;
+    this.state = 'live';
+    if (kind === 'hello' && this.#hellos++ > 0) this.#resyncAll();
+    if (kind === 'reset') {
+      const reason = (data as { reason?: string }).reason;
+      if (reason === 'lagged') this.#resyncAll();
+      if (reason === 'credentials') this.#closed();
+    }
+    for (const fn of this.#listeners.get(kind) ?? []) fn(data);
+  }
+
+  #resyncAll(): void {
+    for (const fn of this.#resync) fn();
+  }
+
+  #closed(): void {
+    this.#source?.close();
+    this.#source = null;
+    const wait = REOPEN_MS[Math.min(this.#reopens++, REOPEN_MS.length - 1)];
+    this.#timer = setTimeout(() => {
+      void this.#deps.check().then((verdict) => {
+        if (verdict === 'ended' || this.#stopped) return;
+        this.#open();
+      });
+    }, wait);
+  }
+}
````

`ui/src/lib/router.svelte.ts`:

````diff
diff --git a/ui/src/lib/router.svelte.ts b/ui/src/lib/router.svelte.ts
new file mode 100644
index 0000000..1b46e85
--- /dev/null
+++ b/ui/src/lib/router.svelte.ts
@@ -0,0 +1,64 @@
+// The console's pages under `/ui/` (Admin_Console §4): each section at its own address, so a
+// link shows the same view; links move between pages without loading the app again, and Back
+// works. The server's own routes under `/ui/` (sign-in, sign-out, the app's files) are left to
+// the browser.
+
+export const SECTIONS = [
+  { id: 'overview', label: 'Overview', path: '/ui/' },
+  { id: 'cluster', label: 'Cluster', path: '/ui/cluster' },
+  { id: 'pipelines', label: 'Pipelines', path: '/ui/pipelines' },
+  { id: 'tables', label: 'Tables', path: '/ui/tables' },
+  { id: 'snapshots', label: 'Snapshots', path: '/ui/snapshots' },
+  { id: 'events', label: 'Events', path: '/ui/events' },
+  { id: 'config', label: 'Config', path: '/ui/config' },
+] as const;
+export type Section = (typeof SECTIONS)[number]['id'];
+
+/** The server's routes under `/ui/`, which aren't the app's pages. */
+const SERVER = ['/ui/sign-in', '/ui/sign-out', '/ui/session', '/ui/oauth2/', '/ui/assets/', '/ui/app/'];
+
+/** The section a path under `/ui/` shows: its first name after `/ui/`, the Overview else. */
+export function sectionOf(path: string): Section {
+  const first = path.replace(/^\/ui\/?/, '').split(/[/?#]/)[0];
+  return SECTIONS.find((s) => s.id === first)?.id ?? 'overview';
+}
+
+/** Whether a click on `a` is the app's to handle: a plain left click on one of its pages. */
+export function isAppLink(a: HTMLAnchorElement, e: MouseEvent): boolean {
+  if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) {
+    return false;
+  }
+  if (a.target !== '' || a.hasAttribute('download') || a.origin !== location.origin) return false;
+  const path = a.pathname;
+  return path.startsWith('/ui/') && !SERVER.some((p) => path === p || path.startsWith(p));
+}
+
+/** Where the page is: its path and query. */
+export class Router {
+  path = $state(location.pathname + location.search);
+  section = $derived(sectionOf(this.path));
+
+  /** Follows the app's links, and Back and Forward; returns the way to stop. */
+  listen(): () => void {
+    const click = (e: MouseEvent) => {
+      const a = (e.target as Element | null)?.closest('a');
+      if (a === null || a === undefined || !isAppLink(a, e)) return;
+      e.preventDefault();
+      this.go(a.pathname + a.search);
+    };
+    const pop = () => (this.path = location.pathname + location.search);
+    document.addEventListener('click', click);
+    window.addEventListener('popstate', pop);
+    return () => {
+      document.removeEventListener('click', click);
+      window.removeEventListener('popstate', pop);
+    };
+  }
+
+  go(to: string): void {
+    if (to === this.path) return;
+    history.pushState(null, '', to);
+    this.path = to;
+    window.scrollTo(0, 0);
+  }
+}
````

`ui/src/lib/session.ts`:

````diff
diff --git a/ui/src/lib/session.ts b/ui/src/lib/session.ts
new file mode 100644
index 0000000..c67a58e
--- /dev/null
+++ b/ui/src/lib/session.ts
@@ -0,0 +1,86 @@
+// Who the console's page acts for (Admin_Console §9.1): `GET /ui/session`, and an oauth2
+// session's token refreshed before it expires.
+
+export type Role = 'viewer' | 'operator' | 'admin';
+export type AuthMode = 'none' | 'basic' | 'oauth2' | 'jwt';
+
+/** `GET /ui/session`: the caller, how they signed in, and the anti-forgery token. */
+export interface Session {
+  name: string;
+  role: Role;
+  auth: AuthMode;
+  /** When the credentials expire (RFC 3339); `null` when they don't. */
+  expires: string | null;
+  signed_in: string | null;
+  /** The session's anti-forgery token, `null` without a session (`none`, `jwt`). */
+  csrf: string | null;
+}
+
+/** How long before its token expires an oauth2 session is refreshed: the server's own lead. */
+export const REFRESH_AHEAD_MS = 60_000;
+/** The wait before trying again when the provider can't be reached and says no `Retry-After`. */
+export const RETRY_MS = 5_000;
+
+/** What the session's calls need: `ended` leaves the page to the server, which says why. */
+export interface Deps {
+  fetch: typeof fetch;
+  ended: () => void;
+}
+
+/** The caller's session; `null` once the page is left to the server (`ended`). */
+export async function readSession(deps: Deps): Promise<Session | null> {
+  const r = await deps.fetch('/ui/session', { headers: { Accept: 'application/json' } });
+  if (r.status === 401) {
+    deps.ended();
+    return null;
+  }
+  if (!r.ok) throw new Error(`the session couldn't be read: ${r.status}`);
+  return (await r.json()) as Session;
+}
+
+/**
+ * Keeps an oauth2 session's token fresh: refreshes it `REFRESH_AHEAD_MS` before it expires. A
+ * provider that can't be reached (503) is asked again after its `Retry-After`; a refusal (401)
+ * ends the session. Returns the way to stop.
+ */
+export function keepFresh(
+  first: Session,
+  deps: Deps,
+  changed: (s: Session) => void,
+): () => void {
+  let timer: ReturnType<typeof setTimeout> | undefined;
+  let stopped = false;
+  const plan = (s: Session) => {
+    if (stopped || s.auth !== 'oauth2' || s.expires === null) return;
+    const wait = Math.max(0, Date.parse(s.expires) - REFRESH_AHEAD_MS - Date.now());
+    timer = setTimeout(() => void refresh(s), wait);
+  };
+  const refresh = async (s: Session) => {
+    let r: Response;
+    try {
+      r = await deps.fetch('/ui/session/refresh', {
+        method: 'POST',
+        headers: { Accept: 'application/json', 'X-CSRF-Token': s.csrf ?? '' },
+      });
+    } catch {
+      timer = setTimeout(() => void refresh(s), RETRY_MS);
+      return;
+    }
+    if (stopped) return;
+    if (r.ok) {
+      const next = (await r.json()) as Session;
+      changed(next);
+      plan(next);
+    } else if (r.status === 503) {
+      const after = Number(r.headers.get('Retry-After'));
+      timer = setTimeout(() => void refresh(s), after > 0 ? after * 1000 : RETRY_MS);
+    } else {
+      deps.ended();
+    }
+  };
+  plan(first);
+  return () => {
+    stopped = true;
+    clearTimeout(timer);
+  };
+}
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cd ui && npm test`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
(cd ui && npm run check && npm test && npm run build)
```

Expected: no output from the first three; every test passes; svelte-check reports 0 errors and
0 warnings.

- [ ] **Step 5: Commit**

```bash
git add ui/src/App.svelte ui/src/Frame.svelte ui/src/Frame.test.ts ui/src/Later.svelte ui/src/app.css ui/src/lib/api.test.ts ui/src/lib/api.ts ui/src/lib/format.test.ts ui/src/lib/format.ts ui/src/lib/live.svelte.ts ui/src/lib/live.test.ts ui/src/lib/router.svelte.ts ui/src/lib/router.test.ts ui/src/lib/session.test.ts ui/src/lib/session.ts ui/vite.config.ts
git commit -m "feat(ui): the console's frame — the session, the /v1 client, the live stream, the sections"
```

---

### Task 4: The Overview (`ui/src/overview`)

**Files:**
- Modify: `ui/src/App.svelte`
- Create: `ui/src/overview/Overview.svelte`
- Create: `ui/src/overview/Overview.test.ts`
- Create: `ui/src/overview/Sparkline.svelte`
- Create: `ui/src/overview/attention.test.ts`
- Create: `ui/src/overview/attention.ts`
- Create: `ui/src/overview/derive.test.ts`
- Create: `ui/src/overview/derive.ts`
- Create: `ui/src/overview/facts.ts`
- Create: `ui/src/overview/fixtures.ts`
- Create: `ui/src/overview/model.svelte.ts`
- Create: `ui/src/overview/model.test.ts`
- Create: `ui/src/overview/types.ts`

**Interfaces:**
- Consumes: Task 3's `Client`, `Live`, `format.ts`; the `/v1` reads of D-M15.7.
- Produces (`facts.ts`): `Facts { pipelines, cluster, config, jobs, tables, figures, lag, events,
  actions, unreachable, unheld }`, `empty()`, `tablesOf(f)`, `lagOf(f, id)`, `at(iso)`.
- Produces (`attention.ts`): `LAG_TARGET_S` (60), `RISING_OVER_S` (300), `WAL_SHARE` (0.5),
  `WAL_UNLIMITED_BYTES` (16 GiB), `PAUSED_FOR_S` (3600), `FAILED_WITHIN_S` (86 400),
  `LEASE_LOSS_WITHIN_S` (3600), `UNHELD_FOR_S` (30); `Attention { severity, what, why, since,
  href, link }`, `attention(f, now): Attention[]`, `walPast(held, limit)`.
- Produces (`derive.ts`): `Tile`, `tiles(f, now)`, `Row`, `rows(f, now)`.
- Produces (`model.svelte.ts`): `EVENTS_SHOWN` (8), `EVENTS_READ` (100), `LAG_SPAN_S` (3600),
  `REREAD_AFTER_MS` (1 000), `PARTS`, `class Overview { facts, read, load(parts?), follow(live),
  pipeline(m), figures(m), samples(m), event(e), peer(m) }`.
- Produces: `Overview.svelte` (props `model`, `now`, `stale`, `last`), `Sparkline.svelte`; `fixtures.ts`,
  mockup 01's cluster for the tests.

- [ ] **Step 1: Write the failing tests**

The tests: `needs attention` (mockup 01's list; each red, orange and yellow rule; the thresholds'
edges, among them a lease without a holder for 29 s and one seen only now; a pipeline that is starting, its figures with no worker and no source yet); `the Overview` tiles
and rows (worst first; a node out of reach; nothing reported yet; a pipeline that is starting); the data (part by part, a part
that fails keeps what it had; the stream's newest owner; since when a lease has no holder; an
event once, a burst read again at once); the page (mockup 01; numbers not current; nothing needs
attention).

`ui/src/overview/Overview.test.ts`:

````diff
diff --git a/ui/src/overview/Overview.test.ts b/ui/src/overview/Overview.test.ts
new file mode 100644
index 0000000..765893c
--- /dev/null
+++ b/ui/src/overview/Overview.test.ts
@@ -0,0 +1,93 @@
+import { flushSync, mount, unmount } from 'svelte';
+import { afterEach, describe, expect, it } from 'vitest';
+import type { Client } from '../lib/api';
+import { clock } from '../lib/format';
+import { facts, NOW } from './fixtures';
+import { Overview as Model } from './model.svelte';
+import Overview from './Overview.svelte';
+
+let mounted: ReturnType<typeof mount> | undefined;
+
+function render(model: Model, stale = false): HTMLElement {
+  const target = document.createElement('div');
+  document.body.replaceChildren(target);
+  mounted = mount(Overview, { target, props: { model, now: NOW, stale, last: NOW - 34_000 } });
+  flushSync();
+  return target;
+}
+
+function model(): Model {
+  const m = new Model({} as Client);
+  m.facts = facts();
+  return m;
+}
+
+/** An element's words, its elements' apart. */
+function text(e: Element | null | undefined): string | undefined {
+  if (e === null || e === undefined) return undefined;
+  const walker = document.createTreeWalker(e, NodeFilter.SHOW_TEXT);
+  const words: string[] = [];
+  for (let n = walker.nextNode(); n !== null; n = walker.nextNode()) {
+    const t = n.textContent?.replace(/\s+/g, ' ').trim();
+    if (t) words.push(t);
+  }
+  return words.join(' ');
+}
+
+describe('the Overview', () => {
+  afterEach(() => {
+    if (mounted) unmount(mounted);
+  });
+
+  it("shows mockup 01's page: tiles, attention, pipelines, events", () => {
+    const page = render(model());
+    expect([...page.querySelectorAll('.tile')].map(text)).toEqual([
+      'Pipelines 4 3 running · 1 paused · 1 halted table',
+      'Worst lag 4m 12s billing-mysql · target 60 s',
+      'Nodes 3 / 3 1 on an older config generation',
+      'Snapshots 1 running · 1 failed today',
+      'WAL held by slots 3.4 GB largest: da_inventory 3.2 GB',
+    ]);
+    expect(text(page.querySelector('h3'))).toBe('Needs attention · 7');
+    const first = page.querySelector('.att li');
+    expect(text(first)).toBe(
+      "orders-pg › public.refunds halted : amount changed int → text, which can't be applied in place 12 min ago Open table →",
+    );
+    expect(first?.querySelector('a')?.getAttribute('href')).toBe('/ui/tables/orders-pg/public.refunds');
+    expect(text(page.querySelector('.rules'))).toContain('Lag target 60 s, rising over 5 min');
+    const rows = [...page.querySelectorAll('tbody tr')];
+    expect(rows.map((r) => r.querySelector('a')?.getAttribute('href'))).toEqual([
+      '/ui/pipelines/orders-pg',
+      '/ui/pipelines/billing-mysql',
+      '/ui/pipelines/customers',
+      '/ui/pipelines/inventory',
+    ]);
+    expect(text(rows[3])).toBe('inventory PostgreSQL → Delta · 1 table paused by Oleksii, 2 h ago n3 — 2 h ago');
+    expect(rows[1].querySelector('polyline')).not.toBeNull();
+    expect([...page.querySelectorAll('.ev li')].map(text)[1]).toBe(
+      `${clock(NOW - 7_200_000)} pause inventory by Oleksii (operator)`,
+    );
+  });
+
+  it("says when its numbers aren't current, and which part couldn't be read", () => {
+    const m = model();
+    m.read.jobs = { at: NOW - 60_000, error: 'the request timed out' };
+    const page = render(m, true);
+    expect(text(page.querySelector('.stale-note'))).toMatch(/^Not current: as of \d\d:\d\d:\d\d · 34 s old$/);
+    expect(page.querySelector('.tiles')?.classList.contains('stale')).toBe(true);
+    expect(text(page.querySelector('.failing'))).toBe(
+      "Couldn't be read, shown as last read: jobs (the request timed out)",
+    );
+  });
+
+  it('says when nothing needs attention', () => {
+    const m = model();
+    m.facts.tables = [];
+    m.facts.figures = {};
+    m.facts.lag = {};
+    m.facts.jobs = [];
+    m.facts.config = null;
+    m.facts.actions = {};
+    expect(text(render(m).querySelector('.att + p, h3 + p'))).toBe('Nothing needs attention.');
+  });
+});
````

`ui/src/overview/attention.test.ts`:

````diff
diff --git a/ui/src/overview/attention.test.ts b/ui/src/overview/attention.test.ts
new file mode 100644
index 0000000..3b5439a
--- /dev/null
+++ b/ui/src/overview/attention.test.ts
@@ -0,0 +1,105 @@
+import { describe, expect, it } from 'vitest';
+import { attention, LAG_TARGET_S } from './attention';
+import { event, facts, figures, iso, NOW } from './fixtures';
+
+const MIN = 60_000;
+const GIB = 1024 ** 3;
+const lines = (f = facts(), now = NOW) => attention(f, now).map((a) => `${a.severity} ${a.what}: ${a.why}`);
+
+describe('needs attention', () => {
+  it("lists mockup 01's cluster red, then orange, then yellow, the newest first", () => {
+    expect(lines()).toEqual([
+      "red orders-pg › public.refunds halted: amount changed int → text, which can't be applied in place",
+      `orange billing-mysql lag 4m 12s: and rising (target ${LAG_TARGET_S} s)`,
+      'orange Snapshot 5b1e2f40 of billing-mysql failed: the source connection was lost',
+      'orange Slot da_inventory holds 3.2 GB of WAL: 80% of max_slot_wal_keep_size · pipeline inventory',
+      'yellow inventory paused: by Oleksii',
+      'yellow Restart-only change waiting on n2: globals.api.address',
+      'yellow n3 runs config generation 41: the cluster is on 42',
+    ]);
+    const [halt, lag] = attention(facts(), NOW);
+    expect([halt.since, halt.href]).toEqual([NOW - 12 * MIN, '/ui/tables/orders-pg/public.refunds']);
+    expect([lag.since, lag.href]).toEqual([NOW - 10 * MIN, '/ui/pipelines/billing-mysql']);
+  });
+
+  it('says when a pipeline errs, a lease has no holder, a node is down, or one is out of reach', () => {
+    const f = facts();
+    f.figures['billing-mysql'].actual = 'error';
+    f.figures['billing-mysql'].error = 'the slot was invalidated';
+    f.cluster!.pipelines[3].holder = null;
+    f.unheld = { customers: NOW - 40_000 };
+    f.cluster!.members[2].suspect = true;
+    f.unreachable = { n2: NOW - 40_000 };
+    f.figures['orders-pg'].worker!.degraded = 'a commit failed: 503 from the store';
+    f.events.unshift(event({ kind: 'lease.takeover', at: iso(NOW - 5 * MIN), pipeline: 'customers', text: 'customers taken over from n1 by n3' }));
+    expect(lines(f).filter((l) => !l.includes('refunds') && !l.startsWith('yellow'))).toEqual([
+      'red customers has no node running it: no node holds its lease',
+      'red billing-mysql stopped by an error: the slot was invalidated',
+      "red n3 is down: the other nodes' probes get no answer",
+      "orange n2 doesn't answer the console: its pipelines show as last known",
+      'orange customers failed over: customers taken over from n1 by n3',
+      `orange billing-mysql lag 4m 12s: and rising (target ${LAG_TARGET_S} s)`,
+      'orange Snapshot 5b1e2f40 of billing-mysql failed: the source connection was lost',
+      'orange Slot da_inventory holds 3.2 GB of WAL: 80% of max_slot_wal_keep_size · pipeline inventory',
+      "orange orders-pg can't commit: a commit failed: 503 from the store",
+    ]);
+  });
+
+  it('keeps to its thresholds', () => {
+    const quiet = (change: (f: ReturnType<typeof facts>) => void) => {
+      const f = facts();
+      change(f);
+      return lines(f);
+    };
+    // The target itself isn't over it; over it but falling isn't rising.
+    expect(quiet((f) => (f.figures['billing-mysql'].worker!.lag_seconds = 60)).join()).not.toContain('lag');
+    expect(
+      quiet((f) => {
+        f.lag['billing-mysql'] = f.lag['billing-mysql'].map(([t], i) => [t, 300 - i * 10]);
+        f.figures['billing-mysql'].worker!.lag_seconds = 180;
+      }).join(),
+    ).not.toContain('lag');
+    // Half of max_slot_wal_keep_size isn't past it; without a limit, 16 GB is the line.
+    const slot = (held: number, limit: number | null) => (f: ReturnType<typeof facts>) => {
+      f.figures.inventory.source = {
+        detail: { engine: 'postgres', slot: { name: 'da_inventory', wal_held_bytes: held }, max_slot_wal_keep_size: limit },
+      };
+    };
+    expect(quiet(slot(2 * GIB, 4 * GIB)).join()).not.toContain('Slot');
+    expect(quiet(slot(16 * GIB, null)).join()).not.toContain('Slot');
+    expect(quiet(slot(16 * GIB + 1, null)).join()).toContain('Slot da_inventory holds 16 GB of WAL: with no max_slot_wal_keep_size');
+    // Paused under an hour ago, or by no person; a job failed over a day ago.
+    expect(quiet((f) => (f.actions.inventory.at = iso(NOW - 59 * MIN))).join()).not.toContain('inventory paused');
+    expect(quiet((f) => delete f.actions.inventory.actor).join()).not.toContain('inventory paused');
+    expect(quiet((f) => (f.jobs[1].finished_at = iso(NOW - 25 * 60 * MIN))).join()).not.toContain('Snapshot');
+    // A lease without a holder for under 30 s is a hand-over; seen only now, it's one too.
+    const unheld = (since: number | null) => (f: ReturnType<typeof facts>) => {
+      f.cluster!.pipelines[3].holder = null;
+      if (since !== null) f.unheld = { customers: since };
+    };
+    expect(quiet(unheld(NOW - 29_000)).join()).not.toContain('no node running it');
+    expect(quiet(unheld(null)).join()).not.toContain('no node running it');
+    expect(quiet(unheld(NOW - 30_000)).join()).toContain('customers has no node running it');
+    // A lease taken over two hours ago is history.
+    expect(
+      quiet((f) => f.events.unshift(event({ kind: 'lease.lost', at: iso(NOW - 61 * MIN), pipeline: 'customers' }))).join(),
+    ).not.toContain('lease');
+  });
+
+  it('takes a pipeline that is starting: no worker, no source yet', () => {
+    const f = facts();
+    f.figures.customers = figures({ id: 'customers', node: 'n1', worker: null, source: null });
+    expect(lines(f)).toEqual(lines());
+  });
+
+  it('is empty when nothing is wrong', () => {
+    const f = facts();
+    f.tables = [];
+    f.figures = { x: figures({ id: 'x', node: 'n1' }) };
+    f.lag = {};
+    f.jobs = [];
+    f.config!.nodes = [];
+    f.actions = {};
+    expect(attention(f, NOW)).toEqual([]);
+  });
+});
````

`ui/src/overview/derive.test.ts`:

````diff
diff --git a/ui/src/overview/derive.test.ts b/ui/src/overview/derive.test.ts
new file mode 100644
index 0000000..4bdb78a
--- /dev/null
+++ b/ui/src/overview/derive.test.ts
@@ -0,0 +1,54 @@
+import { describe, expect, it } from 'vitest';
+import { rows, tiles } from './derive';
+import { facts, figures, NOW } from './fixtures';
+
+describe("the Overview's", () => {
+  it("tiles are mockup 01's", () => {
+    expect(tiles(facts(), NOW).map((t) => [t.key, t.value, t.detail, t.tone])).toEqual([
+      ['Pipelines', '4', '3 running · 1 paused · 1 halted table', 'red'],
+      ['Worst lag', '4m 12s', 'billing-mysql · target 60 s', 'orange'],
+      ['Nodes', '3 / 3', '1 on an older config generation', 'normal'],
+      ['Snapshots', '1', 'running · 1 failed today', 'orange'],
+      ['WAL held by slots', '3.4 GB', 'largest: da_inventory 3.2 GB', 'orange'],
+    ]);
+  });
+
+  it('pipelines are listed the worst first, each with its state, node, lag and last commit', () => {
+    expect(rows(facts(), NOW).map((r) => [r.id, r.what, r.state.label, r.state.note, r.node, r.lag, r.lastCommit])).toEqual([
+      ['orders-pg', 'PostgreSQL → Delta · 2 tables', '1 halted', null, 'n2', 21, NOW - 8_000],
+      ['billing-mysql', 'MySQL → Delta · 1 table', 'lagging', null, 'n1', 252, NOW - 180_000],
+      ['customers', 'Delta · 1 table', 'snapshot 62%', null, 'n1', null, NOW - 12_000],
+      ['inventory', 'PostgreSQL → Delta · 1 table', 'paused', 'by Oleksii, 2 h ago', 'n3', null, NOW - 7_200_000],
+    ]);
+  });
+
+  it("rows of a node out of reach say it, and what was last known", () => {
+    const f = facts();
+    f.unreachable = { n1: NOW - 40_000 };
+    const billing = rows(f, NOW).find((r) => r.id === 'billing-mysql');
+    expect(billing?.state).toEqual({ label: 'unknown', tone: 'grey', note: 'n1 unreachable 40 s · last known: running' });
+  });
+
+  it('tiles say what nothing reports yet', () => {
+    const f = facts();
+    f.figures = {};
+    f.lag = {};
+    f.config = null;
+    const [, lag, nodes, , wal] = tiles(f, NOW);
+    expect([lag.value, lag.detail, nodes.detail, wal.value, wal.detail]).toEqual([
+      '—',
+      'no pipeline reports one · target 60 s',
+      '',
+      '—',
+      'no PostgreSQL slot reported',
+    ]);
+  });
+
+  it('tiles and rows take a pipeline that is starting: no worker, no source yet', () => {
+    const f = facts();
+    f.figures.customers = figures({ id: 'customers', node: 'n1', worker: null, source: null });
+    expect(tiles(f, NOW).find((t) => t.key === 'WAL held by slots')?.value).toBe('3.4 GB');
+    const customers = (f: ReturnType<typeof facts>) => rows(f, NOW).find((r) => r.id === 'customers');
+    expect(customers(f)?.state).toEqual(customers(facts())?.state);
+  });
+});
````

`ui/src/overview/fixtures.ts`:

````diff
diff --git a/ui/src/overview/fixtures.ts b/ui/src/overview/fixtures.ts
new file mode 100644
index 0000000..f943605
--- /dev/null
+++ b/ui/src/overview/fixtures.ts
@@ -0,0 +1,118 @@
+// The tests' cluster: mockup 01's, in the API's shapes.
+
+import type { Facts } from './facts';
+import type { EventItem, Figures } from './types';
+
+export const NOW = Date.UTC(2026, 10, 25, 14, 2, 0);
+const MIN = 60_000;
+const GIB = 1024 ** 3;
+export const iso = (ms: number) => new Date(ms).toISOString();
+
+export function figures(over: Partial<Figures> & Pick<Figures, 'id' | 'node'>): Figures {
+  return {
+    owner_epoch: 2,
+    actual: 'running',
+    as_of: iso(NOW),
+    error: null,
+    source: { detail: null },
+    tables: [],
+    worker: { lag_seconds: 1, degraded: null },
+    ...over,
+  };
+}
+
+export function event(over: Partial<EventItem> & Pick<EventItem, 'kind' | 'at'>): EventItem {
+  return { key: `${over.at}-${over.kind}`, node: 'n1', severity: 'info', text: over.kind, ...over };
+}
+
+const pause = event({
+  kind: 'action.pipeline.pause',
+  at: iso(NOW - 120 * MIN),
+  pipeline: 'inventory',
+  text: 'pause inventory',
+  actor: { name: 'Oleksii', role: 'operator' },
+});
+
+/** Every 5 min over the last hour: `f(minutes ago)`. */
+const line = (f: (ago: number) => number): [number, number][] =>
+  Array.from({ length: 13 }, (_, i) => 60 - i * 5).map((ago) => [(NOW - ago * MIN) / 1000, f(ago)]);
+
+export function facts(): Facts {
+  return {
+    pipelines: [
+      { id: 'orders-pg', name: 'orders-pg', desired_state: 'running', actual_state: 'running', holder: 'n2', holder_stale: false },
+      { id: 'billing-mysql', name: 'billing-mysql', desired_state: 'running', actual_state: 'running', holder: 'n1', holder_stale: false },
+      { id: 'inventory', name: 'inventory', desired_state: 'paused', actual_state: 'paused', holder: 'n3', holder_stale: false },
+      { id: 'customers', name: 'customers', desired_state: 'running', actual_state: 'running', holder: 'n1', holder_stale: false },
+    ],
+    cluster: {
+      node: 'n1',
+      members: ['n1', 'n2', 'n3'].map((node) => ({ node, phase: 'active', suspect: false, config_generation: 42 })),
+      pipelines: [
+        { id: 'orders-pg', holder: 'n2', holder_stale: false, owner_epoch: 2 },
+        { id: 'billing-mysql', holder: 'n1', holder_stale: false, owner_epoch: 3 },
+        { id: 'inventory', holder: 'n3', holder_stale: false, owner_epoch: 1 },
+        { id: 'customers', holder: 'n1', holder_stale: false, owner_epoch: 1 },
+      ],
+    },
+    config: {
+      generation: 42,
+      nodes: [
+        { node: 'n1', generation: 42, file: 'same', restart_pending: [] },
+        { node: 'n2', generation: 42, file: 'same', restart_pending: ['globals.api.address'] },
+        { node: 'n3', generation: 41, file: 'differs', restart_pending: [] },
+      ],
+    },
+    jobs: [
+      { job_id: '7c3a9d5e-0000-4000-8000-000000000001', pipeline: 'customers', status: 'running', rows_done: 62, estimate: 100, requested_at: iso(NOW - 20 * MIN), finished_at: null, error: null },
+      { job_id: '5b1e2f40-0000-4000-8000-000000000002', pipeline: 'billing-mysql', status: 'failed', rows_done: 10, estimate: 50, requested_at: iso(NOW - 200 * MIN), finished_at: iso(NOW - 180 * MIN), error: 'the source connection was lost' },
+    ],
+    tables: [
+      { pipeline: 'orders-pg', table: 'public.refunds', state: 'halted', halt: { code: 'SCHEMA_REFUSED', detail: "amount changed int → text, which can't be applied in place", since: iso(NOW - 12 * MIN) }, last_commit: iso(NOW - 15 * MIN) },
+      { pipeline: 'orders-pg', table: 'public.orders', state: 'streaming', halt: null, last_commit: iso(NOW - 8_000) },
+      { pipeline: 'billing-mysql', table: 'billing.invoices', state: 'streaming', halt: null, last_commit: iso(NOW - 3 * MIN) },
+      { pipeline: 'inventory', table: 'public.items', state: 'streaming', halt: null, last_commit: iso(NOW - 120 * MIN) },
+      { pipeline: 'customers', table: 'public.customers', state: 'snapshot', halt: null, last_commit: iso(NOW - 12_000) },
+    ],
+    figures: {
+      'billing-mysql': figures({
+        id: 'billing-mysql',
+        node: 'n1',
+        owner_epoch: 3,
+        source: { detail: { engine: 'mysql' } },
+        tables: [{ table: 'billing.invoices', state: 'streaming', halt: null, last_commit: iso(NOW - 3 * MIN) }],
+        worker: { lag_seconds: 252, degraded: null },
+      }),
+      inventory: figures({
+        id: 'inventory',
+        node: 'n3',
+        actual: 'paused',
+        owner_epoch: 1,
+        source: { detail: { engine: 'postgres', slot: { name: 'da_inventory', wal_held_bytes: 3.2 * GIB }, max_slot_wal_keep_size: 4 * GIB } },
+        tables: [{ table: 'public.items', state: 'streaming', halt: null, last_commit: iso(NOW - 120 * MIN) }],
+        worker: { lag_seconds: null, degraded: null },
+      }),
+      'orders-pg': figures({
+        id: 'orders-pg',
+        node: 'n2',
+        source: { detail: { engine: 'postgres', slot: { name: 'da_orders', wal_held_bytes: 0.2 * GIB }, max_slot_wal_keep_size: null } },
+        tables: [
+          { table: 'public.refunds', state: 'halted', halt: { code: 'SCHEMA_REFUSED', detail: "amount changed int → text, which can't be applied in place", since: iso(NOW - 12 * MIN) }, last_commit: iso(NOW - 15 * MIN) },
+          { table: 'public.orders', state: 'streaming', halt: null, last_commit: iso(NOW - 8_000) },
+        ],
+        worker: { lag_seconds: 21, degraded: null },
+      }),
+    },
+    lag: {
+      // Over the target since 10 min ago, and rising.
+      'billing-mysql': line((ago) => (ago > 10 ? 10 : 252 - ago * 19)),
+      'orders-pg': line(() => 21),
+    },
+    events: [
+      event({ kind: 'lease.acquired', at: iso(NOW - 7 * MIN), pipeline: 'orders-pg', text: 'orders-pg runs on n2 (owner epoch 2)' }),
+      pause,
+    ],
+    actions: { inventory: pause },
+    unreachable: {},
+  };
+}
````

`ui/src/overview/model.test.ts`:

````diff
diff --git a/ui/src/overview/model.test.ts b/ui/src/overview/model.test.ts
new file mode 100644
index 0000000..da9f02e
--- /dev/null
+++ b/ui/src/overview/model.test.ts
@@ -0,0 +1,115 @@
+import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
+import { HttpError, type Client } from '../lib/api';
+import { event, facts, figures, iso, NOW } from './fixtures';
+import { Overview, REREAD_AFTER_MS } from './model.svelte';
+
+/** A `Client` answering each path from `answers`; a path not there fails. */
+function api(answers: Record<string, unknown>): Client & { asked: string[] } {
+  const asked: string[] = [];
+  return {
+    asked,
+    get: async <T,>(path: string) => {
+      asked.push(path);
+      if (!(path in answers)) throw new HttpError(503, `${path} is unavailable`);
+      return answers[path] as T;
+    },
+    send: async () => {
+      throw new Error('the Overview changes nothing');
+    },
+  };
+}
+
+const READS = {
+  '/v1/pipelines': { pipelines: facts().pipelines },
+  '/v1/cluster/status': facts().cluster,
+  '/v1/config/nodes': facts().config,
+  '/v1/snapshots': { jobs: facts().jobs, unreadable: [] },
+  '/v1/tables': { as_of: iso(NOW), tables: facts().tables, unreachable: [] },
+  '/v1/events?limit=100': { events: facts().events },
+  '/v1/events?kind=action.pipeline.pause,action.pipeline.stop,action.pipeline.resume&limit=1000': {
+    events: [
+      event({ kind: 'action.pipeline.resume', at: iso(NOW - 60_000), pipeline: 'b' }),
+      facts().actions.inventory,
+      event({ kind: 'action.pipeline.pause', at: iso(NOW - 120_000), pipeline: 'b' }),
+    ],
+  },
+  '/v1/history?series=lag_seconds': {
+    series: [{ name: 'lag_seconds', pipeline: 'orders-pg', points: [[NOW / 1000, 21]] }],
+  },
+};
+
+describe("the Overview's data", () => {
+  beforeEach(() => {
+    vi.useFakeTimers();
+    vi.setSystemTime(NOW);
+  });
+  afterEach(() => vi.useRealTimers());
+
+  it('is read part by part; a part that fails keeps what it had and says why', async () => {
+    const o = new Overview(api(READS));
+    await o.load();
+    expect(o.facts.pipelines.map((p) => p.id)).toEqual(['orders-pg', 'billing-mysql', 'inventory', 'customers']);
+    expect(o.facts.actions.b.kind).toBe('action.pipeline.resume');
+    expect(o.facts.lag['orders-pg']).toEqual([[NOW / 1000, 21]]);
+    expect(Object.values(o.read).every((r) => r.at === NOW && r.error === null)).toBe(true);
+    const { '/v1/snapshots': _, ...without } = READS;
+    const later = new Overview(api(without));
+    later.facts.jobs = facts().jobs;
+    await later.load();
+    expect(later.read.jobs).toEqual({ at: null, error: '/v1/snapshots is unavailable' });
+    expect(later.facts.jobs).toHaveLength(2);
+    expect(later.read.pipelines.error).toBeNull();
+  });
+
+  it("follows the stream: the newest owner's figures, a state, the lag's last hour", () => {
+    const o = new Overview(api({}));
+    o.facts = facts();
+    o.figures(figures({ id: 'billing-mysql', node: 'n2', owner_epoch: 2, worker: { lag_seconds: 1, degraded: null } }));
+    expect(o.facts.figures['billing-mysql'].node).toBe('n1');
+    o.figures(figures({ id: 'billing-mysql', node: 'n2', owner_epoch: 4, worker: { lag_seconds: 1, degraded: null } }));
+    expect(o.facts.figures['billing-mysql'].node).toBe('n2');
+    o.pipeline({ pipeline: 'billing-mysql', actual: 'standby', error: null, node: 'n1', owner_epoch: 3 });
+    expect(o.facts.pipelines[1].actual_state).toBe('running');
+    o.pipeline({ pipeline: 'billing-mysql', actual: 'paused', error: null, node: 'n2', owner_epoch: 4 });
+    expect([o.facts.pipelines[1].actual_state, o.facts.figures['billing-mysql'].actual]).toEqual(['paused', 'paused']);
+    o.samples({ scope: 'pipeline', pipeline: 'orders-pg', at: NOW / 1000 + 3600, series: { lag_seconds: 30 } });
+    expect(o.facts.lag['orders-pg'].at(-1)).toEqual([NOW / 1000 + 3600, 30]);
+    expect(o.facts.lag['orders-pg'][0][0]).toBeGreaterThan(NOW / 1000);
+    o.peer({ peer: 'n3', state: 'unreachable', error: 'it answered 503' });
+    expect(o.facts.unreachable).toEqual({ n3: NOW });
+    o.peer({ peer: 'n3', state: 'following' });
+    expect(o.facts.unreachable).toEqual({});
+  });
+
+  it('remembers since when a lease has had no holder, until one holds it', async () => {
+    const status = structuredClone(facts().cluster!);
+    status.pipelines[3].holder = null;
+    const o = new Overview(api({ '/v1/cluster/status': status }));
+    await o.load(['cluster']);
+    expect(o.facts.unheld).toEqual({ customers: NOW });
+    vi.setSystemTime(NOW + 20_000);
+    status.pipelines[2].holder_stale = true;
+    await o.load(['cluster']);
+    expect(o.facts.unheld).toEqual({ customers: NOW, inventory: NOW + 20_000 });
+    status.pipelines[3].holder = 'n2';
+    await o.load(['cluster']);
+    expect(o.facts.unheld).toEqual({ inventory: NOW + 20_000 });
+  });
+
+  it('lists an event once, and reads again what it changed, a burst at once', async () => {
+    const client = api(READS);
+    const o = new Overview(client);
+    o.facts = facts();
+    const paused = event({ kind: 'action.pipeline.pause', at: iso(NOW), pipeline: 'customers', actor: { name: 'ci-bot', role: 'operator' } });
+    o.event(paused);
+    o.event(paused);
+    o.event(event({ kind: 'job.completed', at: iso(NOW), pipeline: 'customers' }));
+    o.event(event({ kind: 'config.published', at: iso(NOW) }));
+    expect(o.facts.events.filter((e) => e.key === paused.key)).toHaveLength(1);
+    expect(o.facts.events[0].kind).toBe('config.published');
+    expect(o.facts.actions.customers).toEqual(paused);
+    expect(client.asked).toEqual([]);
+    await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
+    expect(client.asked.sort()).toEqual(['/v1/config/nodes', '/v1/snapshots']);
+  });
+});
````

- [ ] **Step 2: Run them to see them fail**

Run: `cd ui && npm test`
Expected: four test files fail to resolve `./attention`, `./derive` and `./model.svelte`; the 22 tests of Tasks 1–3 pass.

- [ ] **Step 3: Implement**

Write the facts, the rules, the tiles and rows, the model and the page:

`ui/src/App.svelte`:

````diff
diff --git a/ui/src/App.svelte b/ui/src/App.svelte
index 0babb3d..b764704 100644
--- a/ui/src/App.svelte
+++ b/ui/src/App.svelte
@@ -7,6 +7,8 @@
   import { Live } from './lib/live.svelte';
   import { Router } from './lib/router.svelte';
   import { keepFresh, readSession, type Session } from './lib/session';
+  import { Overview as OverviewModel } from './overview/model.svelte';
+  import Overview from './overview/Overview.svelte';
 
   // The server says why the session ended: its 401 page, or the sign-in.
   const ended = () => location.reload();
@@ -28,6 +30,7 @@
       return r.ok ? 'ok' : 'later';
     },
   });
+  const overview = new OverviewModel(api);
 
   async function readGeneration() {
     try {
@@ -44,6 +47,7 @@
       live.on('event', (e) => {
         if ((e as { kind?: string }).kind === 'config.published') void readGeneration();
       }),
+      overview.follow(live),
     ];
     void readSession({ fetch, ended }).then((s) => {
       if (s === null) return;
@@ -51,6 +55,7 @@
       stops.push(keepFresh(s, { fetch, ended }, (next) => (session = next)));
       live.start();
       void readGeneration();
+      void overview.load();
     });
     const tick = setInterval(() => {
       now = Date.now();
@@ -74,6 +79,10 @@
     {now}
     onRetry={() => live.retry()}
   >
-    <Later section={router.section} />
+    {#if router.section === 'overview'}
+      <Overview model={overview} {now} stale={live.stale(now)} last={live.last} />
+    {:else}
+      <Later section={router.section} />
+    {/if}
   </Frame>
 {/if}
````

`ui/src/overview/Overview.svelte`:

````diff
diff --git a/ui/src/overview/Overview.svelte b/ui/src/overview/Overview.svelte
new file mode 100644
index 0000000..eb488ce
--- /dev/null
+++ b/ui/src/overview/Overview.svelte
@@ -0,0 +1,289 @@
+<script lang="ts">
+  // The Overview (Admin_Console §4, mockup 01): is anything wrong? The headline tiles, what needs
+  // attention, the pipelines worst first with their lag over the last hour, the newest events.
+  import { ago, bytes, clock, duration } from '../lib/format';
+  import {
+    FAILED_WITHIN_S,
+    LAG_TARGET_S,
+    LEASE_LOSS_WITHIN_S,
+    PAUSED_FOR_S,
+    RISING_OVER_S,
+    UNHELD_FOR_S,
+    WAL_SHARE,
+    WAL_UNLIMITED_BYTES,
+    attention,
+  } from './attention';
+  import { rows, tiles } from './derive';
+  import { EVENTS_SHOWN, PARTS, type Overview } from './model.svelte';
+  import Sparkline from './Sparkline.svelte';
+
+  interface Props {
+    model: Overview;
+    now: number;
+    /** The live updates stopped: every number is as of `last`. */
+    stale: boolean;
+    last: number | null;
+  }
+  let { model, now, stale, last }: Props = $props();
+
+  const list = $derived(attention(model.facts, now));
+  const shown = $derived(tiles(model.facts, now));
+  const table = $derived(rows(model.facts, now));
+  const events = $derived(model.facts.events.slice(0, EVENTS_SHOWN));
+  const failing = $derived(
+    PARTS.filter((p) => model.read[p].error !== null).map((p) => `${p} (${model.read[p].error})`),
+  );
+  const asOf = $derived(last ?? Math.max(0, ...PARTS.map((p) => model.read[p].at ?? 0)));
+  const href = (kind: string, id: string) => `/ui/${kind}/${encodeURIComponent(id)}`;
+</script>
+
+<h2>Overview</h2>
+{#if stale}
+  <p class="notice stale-note" role="status">
+    Not current: as of {clock(asOf, true)} · {duration((now - asOf) / 1000)} old
+  </p>
+{/if}
+{#if failing.length > 0}
+  <p class="notice failing" role="alert">Couldn't be read, shown as last read: {failing.join('; ')}</p>
+{/if}
+
+<div class="tiles" class:stale>
+  {#each shown as t (t.key)}
+    <div class="tile {t.tone}">
+      <div class="k">{t.key}</div>
+      <div class="v">{t.value}</div>
+      <div class="s">{t.detail}</div>
+    </div>
+  {/each}
+</div>
+
+<h3>Needs attention · {list.length}</h3>
+{#if list.length === 0}
+  <p class="muted">Nothing needs attention.</p>
+{:else}
+  <ul class="att" class:stale>
+    {#each list as a (a.severity + a.what + a.href)}
+      <li>
+        <span class="dot {a.severity}" title={a.severity}></span>
+        <span class="what"><b>{a.what}</b>: {a.why}</span>
+        <span class="when">{a.since === null ? '' : ago(a.since, now)}</span>
+        <a href={a.href}>{a.link} →</a>
+      </li>
+    {/each}
+  </ul>
+{/if}
+<p class="rules muted">
+  Lag target {LAG_TARGET_S} s, rising over {RISING_OVER_S / 60} min · a slot's WAL past
+  {WAL_SHARE * 100}% of max_slot_wal_keep_size, or {bytes(WAL_UNLIMITED_BYTES)} without one ·
+  paused or stopped by a person for over {PAUSED_FOR_S / 3600} h · snapshot jobs failed in the last
+  {FAILED_WITHIN_S / 3600} h · leases lost in the last {LEASE_LOSS_WITHIN_S / 3600} h, or with no
+  holder for over {UNHELD_FOR_S} s
+</p>
+
+<div class="two">
+  <section>
+    <h3>Pipelines</h3>
+    <table class:stale>
+      <thead>
+        <tr><th>Pipeline</th><th>State</th><th>Node</th><th>Lag, last hour</th><th>Last commit</th></tr>
+      </thead>
+      <tbody>
+        {#each table as r (r.id)}
+          <tr>
+            <td><a href={href('pipelines', r.id)}><b>{r.id}</b></a><div class="muted">{r.what}</div></td>
+            <td>
+              <span class="chip {r.state.tone}">{r.state.label}</span>
+              {#if r.state.note !== null}<div class="muted note">{r.state.note}</div>{/if}
+            </td>
+            <td>{#if r.node !== null}<a href={href('nodes', r.node)}>{r.node}</a>{/if}</td>
+            <td>
+              <Sparkline points={r.points} target={LAG_TARGET_S} />
+              {r.lag === null ? '' : duration(r.lag)}
+            </td>
+            <td>{r.lastCommit === null ? '—' : ago(r.lastCommit, now)}</td>
+          </tr>
+        {/each}
+      </tbody>
+    </table>
+  </section>
+  <section>
+    <h3>Recent events</h3>
+    <ul class="ev" class:stale>
+      {#each events as e (e.key)}
+        <li>
+          <span class="t">{clock(Date.parse(e.at))}</span>
+          <span class="sev {e.severity}">{e.text}</span>
+          {#if e.actor}by <b>{e.actor.name}</b> ({e.actor.role}){/if}
+        </li>
+      {/each}
+    </ul>
+    <a href="/ui/events">All events →</a>
+  </section>
+</div>
+
+<style>
+  h3 {
+    margin: 16px 0 6px;
+    font-size: 12px;
+    text-transform: uppercase;
+    letter-spacing: 0.05em;
+    color: var(--text-2);
+  }
+  .notice {
+    padding: 6px 10px;
+    border-radius: 6px;
+  }
+  .stale-note {
+    background: var(--grey-bg);
+  }
+  .failing {
+    background: var(--orange-bg);
+  }
+  .stale {
+    opacity: 0.55;
+  }
+  .tiles {
+    display: grid;
+    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
+    gap: 10px;
+  }
+  .tile {
+    border: 1px solid var(--border);
+    border-radius: 8px;
+    padding: 10px;
+    background: var(--bg-panel);
+  }
+  .k {
+    font-size: 11px;
+    text-transform: uppercase;
+    letter-spacing: 0.04em;
+    color: var(--text-2);
+  }
+  .v {
+    font-size: 22px;
+    font-weight: 600;
+  }
+  .s {
+    font-size: 11px;
+    color: var(--text-2);
+  }
+  .tile.orange .v {
+    color: var(--orange);
+  }
+  .tile.red .v {
+    color: var(--red);
+  }
+  .att,
+  .ev {
+    list-style: none;
+    margin: 0;
+    padding: 0;
+  }
+  .att {
+    border: 1px solid var(--border);
+    border-radius: 8px;
+    background: var(--bg-panel);
+  }
+  .att li {
+    display: flex;
+    gap: 10px;
+    align-items: center;
+    padding: 7px 10px;
+    border-top: 1px solid var(--border);
+  }
+  .att li:first-child {
+    border-top: 0;
+  }
+  .what {
+    flex: 1;
+  }
+  .when {
+    color: var(--text-2);
+    font-size: 12px;
+  }
+  .dot {
+    width: 9px;
+    height: 9px;
+    border-radius: 50%;
+    flex: none;
+  }
+  .dot.red {
+    background: var(--red);
+  }
+  .dot.orange {
+    background: var(--orange);
+  }
+  .dot.yellow {
+    background: var(--yellow);
+  }
+  .rules {
+    font-size: 11px;
+  }
+  .two {
+    display: grid;
+    grid-template-columns: 1.6fr 1fr;
+    gap: 16px;
+  }
+  table {
+    width: 100%;
+    border-collapse: collapse;
+  }
+  th,
+  td {
+    padding: 6px 8px;
+    border-top: 1px solid var(--border);
+    text-align: left;
+    vertical-align: top;
+  }
+  th {
+    font-weight: 500;
+    color: var(--text-2);
+    border-top: 0;
+    font-size: 11px;
+    text-transform: uppercase;
+  }
+  .chip {
+    display: inline-block;
+    padding: 1px 8px;
+    border-radius: 10px;
+    font-size: 11px;
+    font-weight: 600;
+  }
+  .chip.green {
+    background: var(--green-bg);
+    color: var(--green);
+  }
+  .chip.orange {
+    background: var(--orange-bg);
+    color: var(--orange);
+  }
+  .chip.red {
+    background: var(--red-bg);
+    color: var(--red);
+  }
+  .chip.grey {
+    background: var(--grey-bg);
+    color: var(--text-2);
+  }
+  .note {
+    font-size: 11px;
+  }
+  .ev li {
+    padding: 4px 0;
+    border-top: 1px solid var(--border);
+  }
+  .ev li:first-child {
+    border-top: 0;
+  }
+  .t {
+    color: var(--text-2);
+    display: inline-block;
+    width: 44px;
+  }
+  .sev.error {
+    color: var(--red);
+  }
+  .sev.warning {
+    color: var(--orange);
+  }
+</style>
````

`ui/src/overview/Sparkline.svelte`:

````diff
diff --git a/ui/src/overview/Sparkline.svelte b/ui/src/overview/Sparkline.svelte
new file mode 100644
index 0000000..ec21cd6
--- /dev/null
+++ b/ui/src/overview/Sparkline.svelte
@@ -0,0 +1,48 @@
+<script lang="ts">
+  // A pipeline's lag over the last hour, as a line; the target dashed once the lag reached it.
+  let { points, target }: { points: [number, number][]; target: number } = $props();
+
+  const W = 120;
+  const H = 22;
+  const line = $derived.by(() => {
+    if (points.length < 2) return null;
+    const t0 = points[0][0];
+    const span = Math.max(1, points[points.length - 1][0] - t0);
+    const top = Math.max(...points.map(([, v]) => v), 1);
+    const y = (v: number) => (H - 2 - (v / top) * (H - 4)).toFixed(1);
+    return {
+      points: points.map(([t, v]) => `${(((t - t0) / span) * W).toFixed(1)},${y(v)}`).join(' '),
+      target: top >= target ? y(target) : null,
+      over: points[points.length - 1][1] > target,
+    };
+  });
+</script>
+
+{#if line === null}
+  <span class="muted">—</span>
+{:else}
+  <svg width={W} height={H} viewBox="0 0 {W} {H}" aria-hidden="true">
+    {#if line.target !== null}
+      <line class="target" x1="0" x2={W} y1={line.target} y2={line.target} />
+    {/if}
+    <polyline class:over={line.over} points={line.points} />
+  </svg>
+{/if}
+
+<style>
+  svg {
+    vertical-align: middle;
+  }
+  polyline {
+    fill: none;
+    stroke: var(--green);
+    stroke-width: 1.5;
+  }
+  polyline.over {
+    stroke: var(--orange);
+  }
+  .target {
+    stroke: var(--grey);
+    stroke-dasharray: 3 3;
+  }
+</style>
````

`ui/src/overview/attention.ts`:

````diff
diff --git a/ui/src/overview/attention.ts b/ui/src/overview/attention.ts
new file mode 100644
index 0000000..4983670
--- /dev/null
+++ b/ui/src/overview/attention.ts
@@ -0,0 +1,239 @@
+// Needs attention (Admin_Console §4.1): what is wrong now, red → orange → yellow, each linking
+// to where it's fixed. The thresholds are config-free defaults, stated on the page (Oleksii,
+// 2026-11-25: a later release makes them configurable).
+
+import { bytes, duration } from '../lib/format';
+import { at, lagOf, tablesOf, type Facts } from './facts';
+
+/** The lag a pipeline is expected to stay under. */
+export const LAG_TARGET_S = 60;
+/** "Rising": higher now than this long ago. */
+export const RISING_OVER_S = 300;
+/** A slot holding more than this share of `max_slot_wal_keep_size`… */
+export const WAL_SHARE = 0.5;
+/** …or more than this, when the server sets no limit. */
+export const WAL_UNLIMITED_BYTES = 16 * 1024 ** 3;
+/** A pipeline paused or stopped by a person for longer than this. */
+export const PAUSED_FOR_S = 3600;
+/** A snapshot job that failed within this. */
+export const FAILED_WITHIN_S = 86_400;
+/** A lease lost or taken over within this. */
+export const LEASE_LOSS_WITHIN_S = 3600;
+/** A lease with no holder for longer than this: a rebalance hands leases over within it. */
+export const UNHELD_FOR_S = 30;
+
+export type Severity = 'red' | 'orange' | 'yellow';
+
+export interface Attention {
+  severity: Severity;
+  /** What, in a few words (shown bold), then why. */
+  what: string;
+  why: string;
+  /** Since when (ms), when known. */
+  since: number | null;
+  href: string;
+  link: string;
+}
+
+const RANK: Record<Severity, number> = { red: 0, orange: 1, yellow: 2 };
+
+const pipelineHref = (id: string) => `/ui/pipelines/${encodeURIComponent(id)}`;
+
+/** Whether a slot's WAL held is past its threshold. */
+export function walPast(held: number, limit: number | null | undefined): boolean {
+  return limit === null || limit === undefined ? held > WAL_UNLIMITED_BYTES : held > WAL_SHARE * limit;
+}
+
+/** Everything that needs attention at `now` (ms), the worst first, then the newest. */
+export function attention(f: Facts, now: number): Attention[] {
+  const out: Attention[] = [];
+  const add = (a: Attention) => out.push(a);
+
+  // Red: a table halted; a pipeline stopped by an error; a lease with no holder; a node down.
+  for (const t of tablesOf(f)) {
+    if (t.halt === null) continue;
+    add({
+      severity: 'red',
+      what: `${t.pipeline} › ${t.table} halted`,
+      why: t.halt.detail,
+      since: at(t.halt.since),
+      href: `/ui/tables/${encodeURIComponent(t.pipeline)}/${encodeURIComponent(t.table)}`,
+      link: 'Open table',
+    });
+  }
+  for (const p of f.pipelines) {
+    const live = f.figures[p.id];
+    if ((live?.actual ?? p.actual_state) === 'error') {
+      add({
+        severity: 'red',
+        what: `${p.id} stopped by an error`,
+        why: live?.error ?? 'it needs a stop, then a resume',
+        since: null,
+        href: pipelineHref(p.id),
+        link: 'Open',
+      });
+    }
+  }
+  for (const l of f.cluster?.pipelines ?? []) {
+    const since = f.unheld[l.id];
+    if ((l.holder === null || l.holder_stale) && since !== undefined && now - since >= UNHELD_FOR_S * 1000) {
+      add({
+        severity: 'red',
+        what: `${l.id} has no node running it`,
+        why: l.holder === null ? 'no node holds its lease' : `${l.holder} stopped renewing its lease`,
+        since,
+        href: pipelineHref(l.id),
+        link: 'Open',
+      });
+    }
+  }
+  for (const m of f.cluster?.members ?? []) {
+    if (m.suspect) {
+      add({
+        severity: 'red',
+        what: `${m.node} is down`,
+        why: "the other nodes' probes get no answer",
+        since: null,
+        href: `/ui/nodes/${encodeURIComponent(m.node)}`,
+        link: 'Node',
+      });
+    }
+  }
+
+  // Orange: lag over target and rising; a slot's WAL past its share; lease losses; a failed
+  // snapshot job; sink commits failing; a node the console can't reach.
+  for (const p of f.pipelines) {
+    const lag = lagOf(f, p.id);
+    const points = f.lag[p.id] ?? [];
+    if (lag === null || lag <= LAG_TARGET_S || points.length === 0) continue;
+    const before = points.find(([t]) => t >= now / 1000 - RISING_OVER_S);
+    if (before === undefined || lag <= before[1]) continue;
+    let since = points[points.length - 1][0];
+    for (let i = points.length - 1; i >= 0 && points[i][1] > LAG_TARGET_S; i--) since = points[i][0];
+    add({
+      severity: 'orange',
+      what: `${p.id} lag ${duration(lag)}`,
+      why: `and rising (target ${LAG_TARGET_S} s)`,
+      since: since * 1000,
+      href: pipelineHref(p.id),
+      link: 'Open',
+    });
+  }
+  for (const fig of Object.values(f.figures)) {
+    const d = fig.source?.detail;
+    const held = d?.slot?.wal_held_bytes;
+    if (d?.slot && held !== null && held !== undefined && walPast(held, d.max_slot_wal_keep_size)) {
+      const limit = d.max_slot_wal_keep_size;
+      add({
+        severity: 'orange',
+        what: `Slot ${d.slot.name} holds ${bytes(held)} of WAL`,
+        why:
+          limit === null || limit === undefined
+            ? `with no max_slot_wal_keep_size · pipeline ${fig.id}`
+            : `${Math.round((held / limit) * 100)}% of max_slot_wal_keep_size · pipeline ${fig.id}`,
+        since: null,
+        href: pipelineHref(fig.id),
+        link: 'Open',
+      });
+    }
+    if (fig.worker?.degraded) {
+      add({
+        severity: 'orange',
+        what: `${fig.id} can't commit`,
+        why: fig.worker.degraded,
+        since: null,
+        href: pipelineHref(fig.id),
+        link: 'Open',
+      });
+    }
+  }
+  for (const e of f.events) {
+    const when = at(e.at);
+    if (
+      (e.kind === 'lease.lost' || e.kind === 'lease.takeover') &&
+      when !== null &&
+      now - when <= LEASE_LOSS_WITHIN_S * 1000
+    ) {
+      add({
+        severity: 'orange',
+        what: e.kind === 'lease.lost' ? `${e.pipeline} lost its lease` : `${e.pipeline} failed over`,
+        why: e.text,
+        since: when,
+        href: pipelineHref(e.pipeline ?? ''),
+        link: 'Open',
+      });
+    }
+  }
+  for (const j of f.jobs) {
+    const ended = at(j.finished_at);
+    if (j.status === 'failed' && ended !== null && now - ended <= FAILED_WITHIN_S * 1000) {
+      add({
+        severity: 'orange',
+        what: `Snapshot ${j.job_id.slice(0, 8)} of ${j.pipeline} failed`,
+        why: j.error ?? '',
+        since: ended,
+        href: `/ui/snapshots/${encodeURIComponent(j.job_id)}`,
+        link: 'Open',
+      });
+    }
+  }
+  for (const [node, since] of Object.entries(f.unreachable)) {
+    add({
+      severity: 'orange',
+      what: `${node} doesn't answer the console`,
+      why: 'its pipelines show as last known',
+      since,
+      href: `/ui/nodes/${encodeURIComponent(node)}`,
+      link: 'Node',
+    });
+  }
+
+  // Yellow: nodes on different config generations; restart-only changes waiting; a pipeline
+  // paused or stopped by a person for a while.
+  for (const n of f.config?.nodes ?? []) {
+    if (f.config !== null && n.generation !== null && n.generation !== f.config.generation) {
+      add({
+        severity: 'yellow',
+        what: `${n.node} runs config generation ${n.generation}`,
+        why: `the cluster is on ${f.config.generation}`,
+        since: null,
+        href: `/ui/nodes/${encodeURIComponent(n.node)}`,
+        link: 'Node',
+      });
+    }
+    if (n.restart_pending.length > 0) {
+      add({
+        severity: 'yellow',
+        what: `Restart-only change waiting on ${n.node}`,
+        why: n.restart_pending.join(', '),
+        since: null,
+        href: '/ui/config',
+        link: 'Review',
+      });
+    }
+  }
+  for (const p of f.pipelines) {
+    const a = f.actions[p.id];
+    const when = at(a?.at);
+    if (
+      (p.desired_state === 'paused' || p.desired_state === 'stopped') &&
+      a !== undefined &&
+      a.actor !== undefined &&
+      when !== null &&
+      now - when > PAUSED_FOR_S * 1000
+    ) {
+      add({
+        severity: 'yellow',
+        what: `${p.id} ${p.desired_state}`,
+        why: `by ${a.actor.name}`,
+        since: when,
+        href: pipelineHref(p.id),
+        link: 'Open',
+      });
+    }
+  }
+
+  return out.sort(
+    (a, b) => RANK[a.severity] - RANK[b.severity] || (b.since ?? -Infinity) - (a.since ?? -Infinity),
+  );
+}
````

`ui/src/overview/derive.ts`:

````diff
diff --git a/ui/src/overview/derive.ts b/ui/src/overview/derive.ts
new file mode 100644
index 0000000..4a358d1
--- /dev/null
+++ b/ui/src/overview/derive.ts
@@ -0,0 +1,165 @@
+// The Overview's tiles and its pipelines' rows (mockup 01), from what it knows.
+
+import { ago, bytes, duration } from '../lib/format';
+import { FAILED_WITHIN_S, LAG_TARGET_S, walPast } from './attention';
+import { at, lagOf, tablesOf, type Facts } from './facts';
+
+export type Tone = 'normal' | 'green' | 'orange' | 'red' | 'grey';
+
+export interface Tile {
+  key: string;
+  value: string;
+  detail: string;
+  tone: Tone;
+}
+
+const ACTUALS = ['running', 'paused', 'stopped', 'retrying', 'starting', 'dumped', 'error'] as const;
+
+/** The five headline tiles: pipelines, worst lag, nodes, snapshots, WAL held by slots. */
+export function tiles(f: Facts, now: number): Tile[] {
+  const halted = tablesOf(f).filter((t) => t.halt !== null).length;
+  const actual = (id: string) =>
+    f.figures[id]?.actual ?? f.pipelines.find((p) => p.id === id)?.actual_state;
+  const counts = ACTUALS.map((s) => [s, f.pipelines.filter((p) => actual(p.id) === s).length] as const)
+    .filter(([, n]) => n > 0)
+    .map(([s, n]) => `${n} ${s}`);
+  if (halted > 0) counts.push(`${halted} halted ${halted === 1 ? 'table' : 'tables'}`);
+  const pipelines: Tile = {
+    key: 'Pipelines',
+    value: String(f.pipelines.length),
+    detail: counts.join(' · '),
+    tone: halted > 0 || f.pipelines.some((p) => actual(p.id) === 'error') ? 'red' : 'normal',
+  };
+
+  let worst: { id: string; lag: number } | null = null;
+  for (const p of f.pipelines) {
+    const lag = lagOf(f, p.id);
+    if (lag !== null && (worst === null || lag > worst.lag)) worst = { id: p.id, lag };
+  }
+  const lag: Tile = {
+    key: 'Worst lag',
+    value: worst === null ? '—' : duration(worst.lag),
+    detail: `${worst === null ? 'no pipeline reports one' : worst.id} · target ${LAG_TARGET_S} s`,
+    tone: worst !== null && worst.lag > LAG_TARGET_S ? 'orange' : 'normal',
+  };
+
+  const members = f.cluster?.members ?? [];
+  const older = (f.config?.nodes ?? []).filter(
+    (n) => n.generation !== null && f.config !== null && n.generation < f.config.generation,
+  ).length;
+  const nodes: Tile = {
+    key: 'Nodes',
+    value: `${members.filter((m) => !m.suspect).length} / ${members.length}`,
+    detail:
+      older > 0
+        ? `${older} on an older config generation`
+        : f.config === null
+          ? ''
+          : `all on config generation ${f.config.generation}`,
+    tone: members.some((m) => m.suspect) ? 'red' : 'normal',
+  };
+
+  const running = f.jobs.filter((j) => ['queued', 'running', 'cancelling'].includes(j.status)).length;
+  const failed = f.jobs.filter((j) => {
+    const ended = at(j.finished_at);
+    return j.status === 'failed' && ended !== null && now - ended <= FAILED_WITHIN_S * 1000;
+  }).length;
+  const snapshots: Tile = {
+    key: 'Snapshots',
+    value: String(running),
+    detail: failed > 0 ? `running · ${failed} failed today` : 'running',
+    tone: failed > 0 ? 'orange' : 'normal',
+  };
+
+  const slots = Object.values(f.figures)
+    .map((fig) => fig.source?.detail)
+    .filter((d) => d?.slot?.wal_held_bytes !== null && d?.slot?.wal_held_bytes !== undefined)
+    .map((d) => ({ name: d!.slot!.name, held: d!.slot!.wal_held_bytes!, limit: d!.max_slot_wal_keep_size }));
+  const largest = slots.reduce<(typeof slots)[number] | null>(
+    (a, s) => (a === null || s.held > a.held ? s : a),
+    null,
+  );
+  const wal: Tile = {
+    key: 'WAL held by slots',
+    value: slots.length === 0 ? '—' : bytes(slots.reduce((n, s) => n + s.held, 0)),
+    detail: largest === null ? 'no PostgreSQL slot reported' : `largest: ${largest.name} ${bytes(largest.held)}`,
+    tone: slots.some((s) => walPast(s.held, s.limit)) ? 'orange' : 'normal',
+  };
+
+  return [pipelines, lag, nodes, snapshots, wal];
+}
+
+export interface Row {
+  id: string;
+  /** Source → sink and the tables: `PostgreSQL → Delta · 14 tables`. */
+  what: string;
+  state: { label: string; tone: Tone; note: string | null };
+  node: string | null;
+  lag: number | null;
+  points: [number, number][];
+  lastCommit: number | null;
+}
+
+const ENGINES = { postgres: 'PostgreSQL', mysql: 'MySQL' } as const;
+
+/** Each pipeline's row, the worst first: halted or in error, lagging, unknown, the rest. */
+export function rows(f: Facts, now: number): Row[] {
+  const tables = tablesOf(f);
+  const out = f.pipelines.map((p): Row & { rank: number } => {
+    const fig = f.figures[p.id];
+    const mine = tables.filter((t) => t.pipeline === p.id);
+    const halted = mine.filter((t) => t.halt !== null).length;
+    const actual = fig?.actual ?? p.actual_state;
+    const node = fig?.node ?? p.holder;
+    const lag = lagOf(f, p.id);
+    const job = f.jobs.find((j) => j.pipeline === p.id && j.status === 'running');
+    const detail = fig?.source?.detail;
+    const engine = detail ? ENGINES[detail.engine] : null;
+    const down = node !== null ? f.unreachable[node] : undefined;
+    let state: Row['state'];
+    let rank = 3;
+    if (down !== undefined) {
+      state = {
+        label: 'unknown',
+        tone: 'grey',
+        note: `${node} unreachable ${duration((now - down) / 1000)} · last known: ${actual ?? 'unknown'}`,
+      };
+      rank = 2;
+    } else if (halted > 0) {
+      state = { label: `${halted} halted`, tone: 'red', note: null };
+      rank = 0;
+    } else if (actual === 'error') {
+      state = { label: 'error', tone: 'red', note: fig?.error ?? null };
+      rank = 0;
+    } else if (lag !== null && lag > LAG_TARGET_S) {
+      state = { label: 'lagging', tone: 'orange', note: null };
+      rank = 1;
+    } else if (job !== undefined) {
+      const pct = job.estimate ? Math.min(99, Math.floor((job.rows_done / job.estimate) * 100)) : null;
+      state = { label: pct === null ? 'snapshot' : `snapshot ${pct}%`, tone: 'green', note: null };
+    } else if (actual === 'paused' || actual === 'stopped') {
+      const a = f.actions[p.id];
+      const when = at(a?.at);
+      state = {
+        label: actual,
+        tone: 'grey',
+        note: a?.actor && when !== null ? `by ${a.actor.name}, ${ago(when, now)}` : null,
+      };
+    } else {
+      state = { label: actual ?? 'unknown', tone: actual === 'running' ? 'green' : 'grey', note: null };
+    }
+    const commits = mine.map((t) => at(t.last_commit)).filter((t): t is number => t !== null);
+    return {
+      id: p.id,
+      what: `${engine ? `${engine} → ` : ''}Delta · ${mine.length} ${mine.length === 1 ? 'table' : 'tables'}`,
+      state,
+      node,
+      lag,
+      points: f.lag[p.id] ?? [],
+      lastCommit: commits.length > 0 ? Math.max(...commits) : null,
+      rank,
+    };
+  });
+  out.sort((a, b) => a.rank - b.rank || (b.lag ?? -1) - (a.lag ?? -1) || a.id.localeCompare(b.id));
+  return out.map(({ rank: _rank, ...row }) => row);
+}
````

`ui/src/overview/facts.ts`:

````diff
diff --git a/ui/src/overview/facts.ts b/ui/src/overview/facts.ts
new file mode 100644
index 0000000..43f5f29
--- /dev/null
+++ b/ui/src/overview/facts.ts
@@ -0,0 +1,73 @@
+// What the Overview knows, from its reads and the stream, and what follows from it for one
+// pipeline.
+
+import type {
+  ClusterStatus,
+  ConfigNodes,
+  EventItem,
+  Figures,
+  Job,
+  PipelineItem,
+  TableItem,
+} from './types';
+
+export interface Facts {
+  pipelines: PipelineItem[];
+  cluster: ClusterStatus | null;
+  config: ConfigNodes | null;
+  jobs: Job[];
+  /** Every pipeline's tables, as `GET /v1/tables` read them. */
+  tables: TableItem[];
+  /** Each pipeline's live figures, from the node that runs it. */
+  figures: Record<string, Figures>;
+  /** Each pipeline's lag over the last hour: `[unix seconds, seconds]`, oldest first. */
+  lag: Record<string, [number, number][]>;
+  /** The newest events, newest first. */
+  events: EventItem[];
+  /** Each pipeline's newest pause, stop or resume. */
+  actions: Record<string, EventItem>;
+  /** The nodes the stream can't follow, since when (ms). */
+  unreachable: Record<string, number>;
+  /** The pipelines whose lease no node holds (or renews), since when the page saw it (ms). */
+  unheld: Record<string, number>;
+}
+
+export function empty(): Facts {
+  return {
+    pipelines: [],
+    cluster: null,
+    config: null,
+    jobs: [],
+    tables: [],
+    figures: {},
+    lag: {},
+    events: [],
+    actions: {},
+    unreachable: {},
+    unheld: {},
+  };
+}
+
+/** Every pipeline's tables: from its live figures where the stream sent them, else as read. */
+export function tablesOf(f: Facts): TableItem[] {
+  const live = new Set(Object.keys(f.figures));
+  return [
+    ...f.tables.filter((t) => !live.has(t.pipeline)),
+    ...Object.values(f.figures).flatMap((fig) => fig.tables.map((t) => ({ ...t, pipeline: fig.id }))),
+  ];
+}
+
+/** A pipeline's lag now: its live figures', else the last sample's; `null` when unknown. */
+export function lagOf(f: Facts, id: string): number | null {
+  const live = f.figures[id]?.worker?.lag_seconds;
+  if (live !== undefined && live !== null) return live;
+  const points = f.lag[id];
+  return points !== undefined && points.length > 0 ? points[points.length - 1][1] : null;
+}
+
+/** When an RFC 3339 time is (ms), `null` for none. */
+export function at(t: string | null | undefined): number | null {
+  if (t === null || t === undefined) return null;
+  const ms = Date.parse(t);
+  return Number.isNaN(ms) ? null : ms;
+}
````

`ui/src/overview/fixtures.ts`:

````diff
diff --git a/ui/src/overview/fixtures.ts b/ui/src/overview/fixtures.ts
index f943605..e516712 100644
--- a/ui/src/overview/fixtures.ts
+++ b/ui/src/overview/fixtures.ts
@@ -114,5 +114,6 @@ export function facts(): Facts {
     ],
     actions: { inventory: pause },
     unreachable: {},
+    unheld: {},
   };
 }
````

`ui/src/overview/model.svelte.ts`:

````diff
diff --git a/ui/src/overview/model.svelte.ts b/ui/src/overview/model.svelte.ts
new file mode 100644
index 0000000..224e3c6
--- /dev/null
+++ b/ui/src/overview/model.svelte.ts
@@ -0,0 +1,221 @@
+// The Overview's data (Admin_Console §8): read at once, each part within its own time limit, then
+// kept current by the live stream. A part that can't be read keeps what it last had and says why.
+
+import type { Client } from '../lib/api';
+import type { Live } from '../lib/live.svelte';
+import { empty, type Facts } from './facts';
+import type {
+  Actual,
+  ClusterStatus,
+  ConfigNodes,
+  EventItem,
+  Figures,
+  Job,
+  PipelineItem,
+  Series,
+  Tables,
+} from './types';
+
+/** The events the Overview lists. */
+export const EVENTS_SHOWN = 8;
+/** The events read for the rules (lease losses of the last hour). */
+export const EVENTS_READ = 100;
+/** The span of the pipelines' lag lines. */
+export const LAG_SPAN_S = 3600;
+/** A burst of events reads their parts again once, after this. */
+export const REREAD_AFTER_MS = 1_000;
+
+export type Part = 'pipelines' | 'cluster' | 'config' | 'jobs' | 'tables' | 'events' | 'actions' | 'lag';
+export const PARTS: Part[] = ['pipelines', 'cluster', 'config', 'jobs', 'tables', 'events', 'actions', 'lag'];
+
+/** When a part was last read (ms), and why its last read failed. */
+export interface Read {
+  at: number | null;
+  error: string | null;
+}
+
+const ACTIONS = 'action.pipeline.pause,action.pipeline.stop,action.pipeline.resume';
+
+/** The parts an event's kind changes. */
+function partsOf(kind: string): Part[] {
+  if (kind.startsWith('job.')) return ['jobs'];
+  if (kind.startsWith('config.')) return ['config'];
+  if (kind.startsWith('member.') || kind.startsWith('lease.')) return ['cluster', 'pipelines'];
+  if (kind.startsWith('table.') || kind.startsWith('schema.')) return ['tables'];
+  if (kind.startsWith('pipeline.')) return ['pipelines'];
+  if (kind.startsWith('action.snapshot.')) return ['jobs'];
+  return [];
+}
+
+export class Overview {
+  facts = $state<Facts>(empty());
+  read = $state<Record<Part, Read>>(
+    Object.fromEntries(PARTS.map((p) => [p, { at: null, error: null }])) as Record<Part, Read>,
+  );
+
+  #api: Client;
+  #pending = new Set<Part>();
+  #timer: ReturnType<typeof setTimeout> | undefined;
+
+  constructor(api: Client) {
+    this.#api = api;
+  }
+
+  /** Reads `parts` (every part by default), each on its own. */
+  async load(parts: Part[] = PARTS): Promise<void> {
+    await Promise.all(parts.map((p) => this.#loadPart(p)));
+  }
+
+  /** Follows `live`; returns the way to stop. */
+  follow(live: Live): () => void {
+    const stops = [
+      live.onResync(() => void this.load()),
+      live.on('pipeline', (d) => this.pipeline(d as PipelineMsg)),
+      live.on('figures', (d) => this.figures(d as Figures)),
+      live.on('samples', (d) => this.samples(d as SamplesMsg)),
+      live.on('event', (d) => this.event(d as EventItem)),
+      live.on('members', () => this.#reread(['cluster'])),
+      live.on('peer', (d) => this.peer(d as PeerMsg)),
+    ];
+    return () => {
+      for (const stop of stops) stop();
+      clearTimeout(this.#timer);
+    };
+  }
+
+  /** A pipeline's state changed on the node that runs it; `standby` when it stopped running it. */
+  pipeline(m: PipelineMsg): void {
+    const p = this.facts.pipelines.find((x) => x.id === m.pipeline);
+    if (p === undefined || m.actual === 'standby') return;
+    const current = this.facts.figures[m.pipeline];
+    if (current !== undefined && current.owner_epoch > m.owner_epoch) return;
+    p.actual_state = m.actual;
+    p.holder = m.node;
+    if (current !== undefined) {
+      current.actual = m.actual;
+      current.error = m.error;
+    }
+  }
+
+  /** A pipeline's figures: the newest owner's win (a node killed sends no `standby`). */
+  figures(m: Figures): void {
+    const current = this.facts.figures[m.id];
+    if (current !== undefined && current.owner_epoch > m.owner_epoch) return;
+    this.facts.figures[m.id] = m;
+  }
+
+  /** A pipeline's chart sample: its lag joins the line, which keeps the last hour. */
+  samples(m: SamplesMsg): void {
+    const lag = m.series?.lag_seconds;
+    if (m.scope !== 'pipeline' || m.pipeline === undefined || lag === undefined) return;
+    const points = [...(this.facts.lag[m.pipeline] ?? []), [m.at, lag] as [number, number]];
+    this.facts.lag[m.pipeline] = points.filter(([t]) => t > m.at - LAG_SPAN_S);
+  }
+
+  /** An event, once written: the newest first, and the parts it changes read again. */
+  event(e: EventItem): void {
+    if (this.facts.events.some((x) => x.key === e.key)) return;
+    this.facts.events = [e, ...this.facts.events].slice(0, EVENTS_READ);
+    if (ACTIONS.split(',').includes(e.kind) && e.pipeline !== undefined) {
+      this.facts.actions[e.pipeline] = e;
+    }
+    this.#reread(partsOf(e.kind));
+  }
+
+  /** Another node followed again, or not reachable. */
+  peer(m: PeerMsg): void {
+    if (m.state === 'unreachable') {
+      this.facts.unreachable[m.peer] ??= Date.now();
+    } else {
+      delete this.facts.unreachable[m.peer];
+    }
+  }
+
+  #reread(parts: Part[]): void {
+    for (const p of parts) this.#pending.add(p);
+    if (this.#timer !== undefined || this.#pending.size === 0) return;
+    this.#timer = setTimeout(() => {
+      this.#timer = undefined;
+      const parts = [...this.#pending];
+      this.#pending.clear();
+      void this.load(parts);
+    }, REREAD_AFTER_MS);
+  }
+
+  async #loadPart(part: Part): Promise<void> {
+    try {
+      const f = this.facts;
+      switch (part) {
+        case 'pipelines':
+          f.pipelines = (await this.#api.get<{ pipelines: PipelineItem[] }>('/v1/pipelines')).pipelines;
+          break;
+        case 'cluster': {
+          f.cluster = await this.#api.get<ClusterStatus>('/v1/cluster/status');
+          const unheld: Record<string, number> = {};
+          for (const l of f.cluster.pipelines) {
+            if (l.holder === null || l.holder_stale) unheld[l.id] = f.unheld[l.id] ?? Date.now();
+          }
+          f.unheld = unheld;
+          break;
+        }
+        case 'config':
+          f.config = await this.#api.get<ConfigNodes>('/v1/config/nodes');
+          break;
+        case 'jobs':
+          f.jobs = (await this.#api.get<{ jobs: Job[] }>('/v1/snapshots')).jobs;
+          break;
+        case 'tables':
+          f.tables = (await this.#api.get<Tables>('/v1/tables')).tables;
+          break;
+        case 'events':
+          f.events = (
+            await this.#api.get<{ events: EventItem[] }>(`/v1/events?limit=${EVENTS_READ}`)
+          ).events;
+          break;
+        case 'actions': {
+          const { events } = await this.#api.get<{ events: EventItem[] }>(
+            `/v1/events?kind=${ACTIONS}&limit=1000`,
+          );
+          const newest: Record<string, EventItem> = {};
+          for (const e of events) if (e.pipeline !== undefined) newest[e.pipeline] ??= e;
+          f.actions = newest;
+          break;
+        }
+        case 'lag': {
+          const { series } = await this.#api.get<{ series: Series[] }>('/v1/history?series=lag_seconds');
+          f.lag = Object.fromEntries(
+            series.filter((s) => s.pipeline !== undefined).map((s) => [s.pipeline!, s.points]),
+          );
+          break;
+        }
+      }
+      this.read[part] = { at: Date.now(), error: null };
+    } catch (e) {
+      this.read[part] = { at: this.read[part].at, error: e instanceof Error ? e.message : String(e) };
+    }
+  }
+}
+
+/** The stream's `pipeline` message. */
+export interface PipelineMsg {
+  pipeline: string;
+  actual: Actual | 'standby';
+  error: string | null;
+  node: string;
+  owner_epoch: number;
+}
+
+/** The stream's `samples` message. */
+export interface SamplesMsg {
+  scope: 'pipeline' | 'node';
+  pipeline?: string;
+  at: number;
+  series?: Record<string, number>;
+}
+
+/** The stream's `peer` message. */
+export interface PeerMsg {
+  peer: string;
+  state: 'following' | 'unreachable';
+  error?: string;
+}
````

`ui/src/overview/types.ts`:

````diff
diff --git a/ui/src/overview/types.ts b/ui/src/overview/types.ts
new file mode 100644
index 0000000..7dbee3f
--- /dev/null
+++ b/ui/src/overview/types.ts
@@ -0,0 +1,103 @@
+// What the Overview reads from `/v1` and the live stream: the fields it uses, as the server sends
+// them (the API's reference names the rest).
+
+export type Actual = 'starting' | 'running' | 'paused' | 'stopped' | 'dumped' | 'retrying' | 'error';
+export type Desired = 'running' | 'paused' | 'stopped';
+
+/** `GET /v1/pipelines`'s rows. */
+export interface PipelineItem {
+  id: string;
+  name: string;
+  desired_state: Desired;
+  actual_state: Actual | null;
+  holder: string | null;
+  holder_stale: boolean;
+}
+
+/** `GET /v1/cluster/status`. */
+export interface ClusterStatus {
+  node: string;
+  members: { node: string; phase: string; suspect: boolean; config_generation: number | null }[];
+  pipelines: { id: string; holder: string | null; holder_stale: boolean; owner_epoch: number }[];
+}
+
+/** `GET /v1/config/nodes`. */
+export interface ConfigNodes {
+  generation: number;
+  nodes: { node: string; generation: number | null; file: string; restart_pending: string[] }[];
+}
+
+export interface Halt {
+  code: string;
+  detail: string;
+  since: string | null;
+}
+
+/** A table as `GET /v1/tables` lists it, and as a pipeline's `figures` show it. */
+export interface TableItem {
+  pipeline: string;
+  table: string;
+  state: string;
+  halt: Halt | null;
+  last_commit: string | null;
+}
+
+/** `GET /v1/tables`. */
+export interface Tables {
+  as_of: string;
+  tables: TableItem[];
+  unreachable: { pipeline: string; error?: string }[];
+}
+
+/** A snapshot job, as `GET /v1/snapshots` lists it. */
+export interface Job {
+  job_id: string;
+  pipeline: string;
+  status: 'queued' | 'running' | 'cancelling' | 'completed' | 'failed' | 'cancelled';
+  rows_done: number;
+  estimate: number | null;
+  requested_at: string;
+  finished_at: string | null;
+  error: string | null;
+}
+
+/** PostgreSQL's source detail; MySQL's has no slot. */
+export interface SourceDetail {
+  engine: 'postgres' | 'mysql';
+  slot?: { name: string; wal_held_bytes: number | null } | null;
+  max_slot_wal_keep_size?: number | null;
+}
+
+/** A pipeline's live figures: `GET /v1/pipelines/{id}/live`, and the stream's `figures`. */
+export interface Figures {
+  id: string;
+  node: string;
+  owner_epoch: number;
+  actual: Actual;
+  as_of: string;
+  error: string | null;
+  /** `null` without a worker: the pipeline is stopped, or starting. */
+  source: { detail: SourceDetail | null } | null;
+  tables: Omit<TableItem, 'pipeline'>[];
+  worker: { lag_seconds: number | null; degraded: string | null } | null;
+}
+
+/** An event, as `GET /v1/events` lists it and the stream's `event` sends it. */
+export interface EventItem {
+  key: string;
+  at: string;
+  kind: string;
+  node: string;
+  pipeline?: string;
+  severity: 'info' | 'warning' | 'error';
+  text: string;
+  actor?: { name: string; role: string };
+  note?: string;
+}
+
+/** `GET /v1/history`'s series: points `[unix seconds, value]`, oldest first. */
+export interface Series {
+  name: string;
+  pipeline?: string;
+  points: [number, number][];
+}
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cd ui && npm test`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
(cd ui && npm run check && npm test && npm run build)
```

Expected: no output from the first three; every test passes; svelte-check reports 0 errors and
0 warnings.

- [ ] **Step 5: Commit**

```bash
git add ui/src/App.svelte ui/src/overview/Overview.svelte ui/src/overview/Overview.test.ts ui/src/overview/Sparkline.svelte ui/src/overview/attention.test.ts ui/src/overview/attention.ts ui/src/overview/derive.test.ts ui/src/overview/derive.ts ui/src/overview/facts.ts ui/src/overview/fixtures.ts ui/src/overview/model.svelte.ts ui/src/overview/model.test.ts ui/src/overview/types.ts
git commit -m "feat(ui): the Overview — tiles, needs attention, the pipelines worst first, the newest events"
```

---

### Task 5: The flows in a browser (`ui/e2e`, the E2E suite, `/v1/pipelines`)

**Files:**
- Modify: `crates/data-avalanche-api/src/pipelines.rs`
- Create: `crates/data-avalanche/tests/e2e/console_app.rs`
- Modify: `crates/data-avalanche/tests/e2e/console_sessions.rs`
- Modify: `crates/data-avalanche/tests/e2e/idp.rs`
- Modify: `crates/data-avalanche/tests/e2e/main.rs`
- Modify: `test-infra/compose.yaml`
- Create: `ui/e2e/basic.spec.ts`
- Create: `ui/e2e/budget.spec.ts`
- Create: `ui/e2e/common.ts`
- Create: `ui/e2e/jwt.spec.ts`
- Create: `ui/e2e/none.spec.ts`
- Create: `ui/e2e/oauth2.spec.ts`
- Create: `ui/e2e/off.spec.ts`
- Regenerate: `ui/package-lock.json`
- Modify: `ui/package.json`
- Create: `ui/playwright.config.ts`
- Modify: `ui/tsconfig.json`

**Interfaces:**
- Consumes: Tasks 2–4 (the app built in); M14's sign-in and pages; the E2E harness (`Cluster`,
  `E2e`, `Proc`, `eventually`), `idp.rs`'s `Idp` (jwt) and `Provider` (oauth2).
- Produces (`ui/e2e/common.ts`): `env(name)`, `signIn(page, user, password)`, `row(page, id)`,
  `act(page, path, token = true): Promise<number>`, `overview(page, name, role)`; `playwright.config.ts` (`DA_CONSOLE_URL`,
  Chromium, one worker).
- Produces (`console_app.rs`): `require_browser!()` (`DATA_AVALANCHE_BROWSER`),
  `playwright(spec, port, env)`; `console_sessions::session_key` becomes `pub(crate)`.
- Produces (`idp.rs`): the `Provider` answers `GET /authorize?…` with a `302` to its approval
  (`approval(issuer, issued, codes, location)`, shared with `approve`).
- Produces (`pipelines.rs`): `list` reads its pipelines [`READ_AT_ONCE`] (16) at a time, in id
  order, as `/v1/snapshots` does; the answer is unchanged.

- [ ] **Step 1: Write the failing tests**

The tests (`console_app.rs`, each running its spec): `basic_sign_in_and_the_live_overview_in_a_browser`
(the sign-in and back; an action shown as it happens; signing out, and a session that no longer
opens), `oauth2_sign_in_in_a_browser`, `jwt_tokens_in_a_browser` (a viewer; roles mapped to none;
no token), `no_authentication_and_the_console_off_in_a_browser`,
`the_first_page_is_drawn_within_its_budget` (50 pipelines; the time since navigation taken in
the page, at the first frame that shows them all, under 1 000 ms). `@types/node` joins
the devDependencies (`npm install` in `ui/` updates the lock), and `tsconfig.json` includes
`e2e/`.

`crates/data-avalanche/tests/e2e/console_app.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/console_app.rs b/crates/data-avalanche/tests/e2e/console_app.rs
new file mode 100644
index 0000000..d24fc9e
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/console_app.rs
@@ -0,0 +1,206 @@
+//! The console's app in a browser (Admin_Console §10; M15): Playwright's flows (`ui/e2e`) against
+//! a node under each `globals.api.auth` mode, the console switched off, and the first page drawn
+//! within its budget (§8) with 50 pipelines on three nodes. Needs `DATA_AVALANCHE_BROWSER=1`, the
+//! app built into the binary (`npm ci && npm run build` in `ui/`, then the binary) and Playwright's
+//! Chromium (`npx playwright install chromium` in `ui/`); without the variable each skips itself.
+
+use std::path::PathBuf;
+use std::time::Duration;
+
+use data_avalanche_testkit::require_it;
+
+use crate::console_sessions::session_key;
+use crate::harness::{Doc, E2e, Options, Proc, eventually};
+use crate::idp::{Idp, Provider};
+use crate::nodes::{Cluster, ITEMS};
+
+/// The pipelines of the budget's cluster (Admin_Console §8).
+const PIPELINES: usize = 50;
+
+/// Returns early unless the browser's flows are asked for.
+macro_rules! require_browser {
+    () => {
+        if std::env::var("DATA_AVALANCHE_BROWSER").as_deref() != Ok("1") {
+            eprintln!("skipped: set DATA_AVALANCHE_BROWSER=1 to run the console in a browser");
+            return;
+        }
+    };
+}
+
+/// Runs `spec`'s flows (under `ui/e2e`) against the node at `port`, with `env`.
+async fn playwright(spec: &str, port: u16, env: &[(&str, &str)]) {
+    let ui = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../ui");
+    let status = tokio::process::Command::new("npx")
+        .args(["playwright", "test", spec])
+        .current_dir(&ui)
+        .env("DA_CONSOLE_URL", format!("http://127.0.0.1:{port}"))
+        .envs(env.iter().copied())
+        .status()
+        .await
+        .expect("npx runs");
+    assert!(status.success(), "{spec}: the browser's flows failed");
+}
+
+/// `globals.console`: on, with a session key made at run time and `more`.
+fn console(more: &str) -> String {
+    format!(
+        r#"console: {{ enabled: true, session_key: "{}"{more} }}"#,
+        session_key()
+    )
+}
+
+/// A node running `test`'s pipeline, its rows landed, with `globals.api` entries `api` and the
+/// `globals` line given.
+async fn node(test: &str, api: &str, globals: &str) -> (E2e, Proc) {
+    let e = E2e::new(
+        test,
+        ITEMS,
+        Options {
+            tables: ", snapshots: { mode: never }",
+            api,
+            globals,
+            ..Options::default()
+        },
+    )
+    .await;
+    let mut p = e.start(&[]);
+    p.ready().await;
+    e.streaming().await;
+    e.insert_series("items", 5, Doc::Row).await;
+    e.converged("items").await;
+    (e, p)
+}
+
+/// `basic`: the sign-in form and back to the page asked for; the Overview live as a pause
+/// happens, the anti-forgery token required; the sign-out; a session that no longer opens.
+#[tokio::test]
+async fn basic_sign_in_and_the_live_overview_in_a_browser() {
+    require_it!();
+    require_browser!();
+    let (e, p) = node(
+        "console_app_basic",
+        ", auth: basic, user: ops, password: s3cret",
+        &console(""),
+    )
+    .await;
+    let env = [
+        ("DA_USER", "ops"),
+        ("DA_PASSWORD", "s3cret"),
+        ("DA_PIPELINE", e.test.as_str()),
+    ];
+    playwright("basic.spec.ts", p.port(), &env).await;
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
+
+/// `oauth2`: the provider's sign-in and back; the sign-out taken on to the provider.
+#[tokio::test]
+async fn oauth2_sign_in_in_a_browser() {
+    require_it!();
+    require_browser!();
+    let provider = Provider::start(&["operator"]).await;
+    let (e, p) = node(
+        "console_app_oauth2",
+        &provider.api(),
+        &console(r#", public_url: "http://127.0.0.1:${E2E_API_PORT}""#),
+    )
+    .await;
+    playwright(
+        "oauth2.spec.ts",
+        p.port(),
+        &[("DA_PIPELINE", e.test.as_str())],
+    )
+    .await;
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
+
+/// `jwt`: the proxy's token on every request; without one, "No valid token"; one whose roles
+/// map to none, "Forbidden".
+#[tokio::test]
+async fn jwt_tokens_in_a_browser() {
+    require_it!();
+    require_browser!();
+    let idp = Idp::start().await;
+    let (e, p) = node(
+        "console_app_jwt",
+        &idp.api(""),
+        "console: { enabled: true }",
+    )
+    .await;
+    let (viewer, unmapped) = (idp.token(&["viewer"]), idp.token(&["nobody"]));
+    let env = [
+        ("DA_VIEWER_TOKEN", viewer.as_str()),
+        ("DA_UNMAPPED_TOKEN", unmapped.as_str()),
+    ];
+    playwright("jwt.spec.ts", p.port(), &env).await;
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
+
+/// `none`: everyone is admin, with the banner; the console off serves nothing under `/ui/`.
+#[tokio::test]
+async fn no_authentication_and_the_console_off_in_a_browser() {
+    require_it!();
+    require_browser!();
+    let (e, p) = node("console_app_none", "", "console: { enabled: true }").await;
+    playwright("none.spec.ts", p.port(), &[]).await;
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+    let (e, p) = node("console_app_off", "", "").await;
+    playwright("off.spec.ts", p.port(), &[]).await;
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
+
+/// Admin_Console §8: the Overview drawn under 1 s with 50 pipelines on three nodes.
+#[tokio::test]
+async fn the_first_page_is_drawn_within_its_budget() {
+    require_it!();
+    require_browser!();
+    let more: String = (1..PIPELINES)
+        .map(|i| {
+            format!(
+                "  p{i:02}:\n    id: p{i:02}\n@SOURCE(_p{i:02})@\n    tables: {{ include: ['@INCLUDE@'], snapshots: {{ mode: never }} }}\n    sink: {{ type: delta, bucket: @BUCKET@, prefix: @PREFIX@/lake_p{i:02}, delta: {{}}, flush: {{ seconds: 1 }} }}\n"
+            )
+        })
+        .collect();
+    let mut c = Cluster::with(
+        "console_app_budget",
+        ITEMS,
+        &more,
+        Options {
+            tables: ", snapshots: { mode: never }",
+            cluster: ", rebalance_stabilization: 3s",
+            globals: "console: { enabled: true }",
+            ..Options::default()
+        },
+    )
+    .await;
+    for i in 1..PIPELINES {
+        c.e.drop_source(&format!("_p{i:02}")).await;
+    }
+    // Every pipeline runs on a node; whether the ring has balanced them yet doesn't matter here.
+    for i in 0..c.nodes.len() {
+        c.start(i).await;
+    }
+    eventually(
+        "every pipeline runs on a node",
+        Duration::from_secs(180),
+        async || {
+            let (_, v) = c.proc("n1").call("GET", "/v1/cluster/status").await;
+            v["pipelines"]
+                .as_array()
+                .is_some_and(|p| p.len() == PIPELINES && p.iter().all(|l| l["holder"].is_string()))
+        },
+    )
+    .await;
+    let n = PIPELINES.to_string();
+    playwright(
+        "budget.spec.ts",
+        c.proc("n1").port(),
+        &[("DA_PIPELINES", &n)],
+    )
+    .await;
+    c.stop().await;
+}
````

`crates/data-avalanche/tests/e2e/console_sessions.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/console_sessions.rs b/crates/data-avalanche/tests/e2e/console_sessions.rs
index f4f1e3c..4e4719f 100644
--- a/crates/data-avalanche/tests/e2e/console_sessions.rs
+++ b/crates/data-avalanche/tests/e2e/console_sessions.rs
@@ -17,7 +17,7 @@ use crate::idp::{Provider, query_value};
 use crate::nodes::{Cluster, ITEMS};
 
 /// A session key made at run time: 48 random bytes.
-fn session_key() -> String {
+pub(crate) fn session_key() -> String {
     let mut b = [0u8; 48];
     std::fs::File::open("/dev/urandom")
         .and_then(|mut f| f.read_exact(&mut b))
````

`crates/data-avalanche/tests/e2e/main.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index 0509668..2c45273 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -8,6 +8,7 @@
 
 mod api;
 mod cluster;
+mod console_app;
 mod console_history;
 mod console_nodes;
 mod console_pipelines;
````

`ui/e2e/basic.spec.ts`:

````diff
diff --git a/ui/e2e/basic.spec.ts b/ui/e2e/basic.spec.ts
new file mode 100644
index 0000000..4ab6532
--- /dev/null
+++ b/ui/e2e/basic.spec.ts
@@ -0,0 +1,59 @@
+import { expect, test } from '@playwright/test';
+import { act, env, overview, row, signIn } from './common';
+
+const user = env('DA_USER');
+const password = env('DA_PASSWORD');
+const pipeline = env('DA_PIPELINE');
+
+test('a page asked for without a session goes to the sign-in, then back to it', async ({ page }) => {
+  await page.goto('/ui/pipelines');
+  await expect(page).toHaveURL(/\/ui\/sign-in\?next=%2Fui%2Fpipelines$/);
+  await signIn(page, user, 'not the password');
+  await expect(page.locator('[role="alert"]')).toBeVisible();
+  await signIn(page, user, password);
+  await expect(page).toHaveURL(/\/ui\/pipelines$/);
+  await expect(page.locator('nav [aria-current="page"]')).toHaveText('Pipelines');
+  await expect(page.locator('main')).toContainText("This page isn't in this build yet.");
+  await page.click('nav >> text=Overview');
+  await expect(page).toHaveURL(/\/ui\/$/);
+  await overview(page, user, 'admin');
+});
+
+test('the Overview is live: an action shows as it happens', async ({ page }) => {
+  await page.goto('/ui/');
+  await signIn(page, user, password);
+  await overview(page, user, 'admin');
+  await expect(page.locator('.tile .v').first()).toHaveText('1');
+  await expect(row(page, pipeline).locator('.chip')).toHaveText('running');
+  // Without the anti-forgery token the session may not act; with it, it does.
+  expect(await act(page, `/v1/pipelines/${pipeline}/pause`, false)).toBe(403);
+  expect(await act(page, `/v1/pipelines/${pipeline}/pause`)).toBe(202);
+  await expect(row(page, pipeline).locator('.chip')).toHaveText('paused', { timeout: 15_000 });
+  await expect(page.locator('.ev')).toContainText(`pause ${pipeline} by ${user} (admin)`);
+  expect(await act(page, `/v1/pipelines/${pipeline}/resume`)).toBe(202);
+  await expect(row(page, pipeline).locator('.chip')).toHaveText('running', { timeout: 15_000 });
+});
+
+test('signing out ends the session; a session that no longer opens says why', async ({
+  page,
+  context,
+}) => {
+  await page.goto('/ui/');
+  await signIn(page, user, password);
+  await overview(page, user, 'admin');
+  await page.click('text=Sign out');
+  await expect(page).toHaveURL(/\/ui\/sign-in\?signed_out$/);
+  await page.goto('/ui/');
+  await expect(page).toHaveURL(/\/ui\/sign-in\?next=/);
+  // Signed in again, the session's cookie altered: it no longer opens.
+  await signIn(page, user, password);
+  await overview(page, user, 'admin');
+  const altered = (await context.cookies()).map((c) =>
+    c.name === '__Host-da_session' ? { ...c, value: `${c.value.slice(0, -4)}AAAA` } : c,
+  );
+  await context.clearCookies();
+  await context.addCookies(altered);
+  const ended = await page.goto('/ui/');
+  expect(ended?.status()).toBe(401);
+  await expect(page.locator('.title')).toHaveText('Your session has ended');
+});
````

`ui/e2e/budget.spec.ts`:

````diff
diff --git a/ui/e2e/budget.spec.ts b/ui/e2e/budget.spec.ts
new file mode 100644
index 0000000..10bd2aa
--- /dev/null
+++ b/ui/e2e/budget.spec.ts
@@ -0,0 +1,26 @@
+import { expect, test } from '@playwright/test';
+import { env } from './common';
+
+/** Admin_Console §8: the first page drawn under 1 s, with 50 pipelines. */
+const FIRST_PAGE_MS = 1_000;
+
+test(`the Overview is drawn within ${FIRST_PAGE_MS} ms of asking for it`, async ({ page }) => {
+  const pipelines = env('DA_PIPELINES');
+  // Once, for the browser's cache of the app's files (kept for good, as a returning person's).
+  await page.goto('/ui/');
+  await expect(page.locator('tbody tr')).toHaveCount(Number(pipelines));
+  await page.goto('/ui/');
+  // The time since navigation at the first frame that shows every pipeline, taken in the page:
+  // the test's own polling isn't the page's.
+  const drawn = await page.waitForFunction(
+    (n) =>
+      document.querySelectorAll('tbody tr').length === n &&
+      document.querySelector('.tile .v')?.textContent === String(n) &&
+      performance.now(),
+    Number(pipelines),
+    { polling: 'raf' },
+  );
+  const ms = Number(await drawn.jsonValue());
+  console.log(`drawn in ${Math.round(ms)} ms`);
+  expect(ms).toBeLessThan(FIRST_PAGE_MS);
+});
````

`ui/e2e/common.ts`:

````diff
diff --git a/ui/e2e/common.ts b/ui/e2e/common.ts
new file mode 100644
index 0000000..559a66e
--- /dev/null
+++ b/ui/e2e/common.ts
@@ -0,0 +1,40 @@
+import { expect, type Page } from '@playwright/test';
+
+/** A value the E2E suite passes; the flows don't run without it. */
+export function env(name: string): string {
+  const v = process.env[name];
+  if (v === undefined || v === '') throw new Error(`${name} isn't set: run the flows through the E2E suite`);
+  return v;
+}
+
+/** Signs in through the `basic` form the page shows. */
+export async function signIn(page: Page, user: string, password: string): Promise<void> {
+  await page.fill('input[name="user"]', user);
+  await page.fill('input[name="password"]', password);
+  await page.click('button[type="submit"]');
+}
+
+/** The Overview's pipeline row of `id`. */
+export function row(page: Page, id: string) {
+  return page.locator('tbody tr', { has: page.locator(`a[href="/ui/pipelines/${id}"]`) });
+}
+
+/** A call the page makes to `/v1` with its session's anti-forgery token, or without it. */
+export async function act(page: Page, path: string, token = true): Promise<number> {
+  return page.evaluate(
+    async ({ path, token }) => {
+      const s = (await (await fetch('/ui/session')).json()) as { csrf: string | null };
+      const headers: Record<string, string> = token && s.csrf ? { 'X-CSRF-Token': s.csrf } : {};
+      return (await fetch(path, { method: 'POST', headers })).status;
+    },
+    { path, token },
+  );
+}
+
+/** The page shows the Overview, live, for `name` with `role`. */
+export async function overview(page: Page, name: string, role: string): Promise<void> {
+  await expect(page.locator('h2')).toHaveText('Overview');
+  await expect(page.locator('.who')).toContainText(name);
+  await expect(page.locator('.who b')).toHaveText(role);
+  await expect(page.locator('.conn')).toHaveText('live', { timeout: 15_000 });
+}
````

`ui/e2e/jwt.spec.ts`:

````diff
diff --git a/ui/e2e/jwt.spec.ts b/ui/e2e/jwt.spec.ts
new file mode 100644
index 0000000..f01b670
--- /dev/null
+++ b/ui/e2e/jwt.spec.ts
@@ -0,0 +1,28 @@
+import { expect, test } from '@playwright/test';
+import { env, overview } from './common';
+
+const header = (token: string) => ({ Authorization: `Bearer ${token}` });
+
+test.describe('with the proxy sending a viewer token', () => {
+  test.use({ extraHTTPHeaders: header(env('DA_VIEWER_TOKEN')) });
+  test('the Overview is shown, with no sign-out', async ({ page }) => {
+    await page.goto('/ui/');
+    await overview(page, 'alice', 'viewer');
+    await expect(page.locator('form[action="/ui/sign-out"]')).toHaveCount(0);
+  });
+});
+
+test.describe('with a token whose roles map to none', () => {
+  test.use({ extraHTTPHeaders: header(env('DA_UNMAPPED_TOKEN')) });
+  test('the page says Forbidden', async ({ page }) => {
+    const r = await page.goto('/ui/');
+    expect(r?.status()).toBe(403);
+    await expect(page.locator('.title')).toHaveText('Forbidden');
+  });
+});
+
+test('without a token the page says so', async ({ page }) => {
+  const r = await page.goto('/ui/');
+  expect(r?.status()).toBe(401);
+  await expect(page.locator('.title')).toHaveText('No valid token');
+});
````

`ui/e2e/none.spec.ts`:

````diff
diff --git a/ui/e2e/none.spec.ts b/ui/e2e/none.spec.ts
new file mode 100644
index 0000000..c84e16a
--- /dev/null
+++ b/ui/e2e/none.spec.ts
@@ -0,0 +1,10 @@
+import { expect, test } from '@playwright/test';
+import { overview } from './common';
+
+test('without authentication everyone is admin, and the page says so', async ({ page }) => {
+  await page.goto('/ui/');
+  await overview(page, 'anonymous', 'admin');
+  await expect(page.locator('.banner')).toHaveText(
+    'This cluster has no authentication: everyone who reaches the console is admin.',
+  );
+});
````

`ui/e2e/oauth2.spec.ts`:

````diff
diff --git a/ui/e2e/oauth2.spec.ts b/ui/e2e/oauth2.spec.ts
new file mode 100644
index 0000000..912f8cf
--- /dev/null
+++ b/ui/e2e/oauth2.spec.ts
@@ -0,0 +1,18 @@
+import { expect, test } from '@playwright/test';
+import { env, overview, row } from './common';
+
+const pipeline = env('DA_PIPELINE');
+
+test("the provider's sign-in comes back to the console, and the sign-out goes on to it", async ({
+  page,
+}) => {
+  await page.goto('/ui/');
+  await expect(page).toHaveURL(/\/ui\/sign-in\?next=/);
+  await page.click('#continue');
+  await overview(page, 'alice.smith', 'operator');
+  await expect(row(page, pipeline).locator('.chip')).toHaveText('running');
+  await page.click('text=Sign out');
+  await expect(page).toHaveURL(/\/end-session\?/);
+  await page.goto('/ui/');
+  await expect(page).toHaveURL(/\/ui\/sign-in\?next=/);
+});
````

`ui/e2e/off.spec.ts`:

````diff
diff --git a/ui/e2e/off.spec.ts b/ui/e2e/off.spec.ts
new file mode 100644
index 0000000..55c4be7
--- /dev/null
+++ b/ui/e2e/off.spec.ts
@@ -0,0 +1,9 @@
+import { expect, test } from '@playwright/test';
+
+test('a cluster with the console off serves none of it', async ({ page }) => {
+  for (const path of ['/ui/', '/ui/sign-in', '/ui/app/index.js']) {
+    const r = await page.goto(path);
+    expect(r?.status()).toBe(404);
+    await expect(page.locator('body')).toContainText('globals.console.enabled');
+  }
+});
````

`ui/package.json`:

````diff
diff --git a/ui/package.json b/ui/package.json
index f5d5c89..51a2f77 100644
--- a/ui/package.json
+++ b/ui/package.json
@@ -11,6 +11,7 @@
   "devDependencies": {
     "@playwright/test": "1.65.1",
     "@sveltejs/vite-plugin-svelte": "7.3.2",
+    "@types/node": "26.6.10",
     "jsdom": "31.0.1",
     "svelte": "5.59.1",
     "svelte-check": "4.7.8",
````

`ui/playwright.config.ts`:

````diff
diff --git a/ui/playwright.config.ts b/ui/playwright.config.ts
new file mode 100644
index 0000000..063c09e
--- /dev/null
+++ b/ui/playwright.config.ts
@@ -0,0 +1,14 @@
+import { defineConfig, devices } from '@playwright/test';
+
+// The console's flows in a browser (Admin_Console §10). The E2E suite starts the cluster each
+// file needs and gives its address in DA_CONSOLE_URL (crates/data-avalanche/tests/e2e/
+// console_app.rs): run them through it, not alone.
+export default defineConfig({
+  testDir: 'e2e',
+  workers: 1,
+  retries: 0,
+  timeout: 60_000,
+  reporter: 'list',
+  use: { baseURL: process.env.DA_CONSOLE_URL, trace: 'retain-on-failure' },
+  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
+});
````

`ui/tsconfig.json`:

````diff
diff --git a/ui/tsconfig.json b/ui/tsconfig.json
index 7d7bcf2..a1f7e00 100644
--- a/ui/tsconfig.json
+++ b/ui/tsconfig.json
@@ -15,7 +15,8 @@
     "skipLibCheck": true,
     "types": [
       "svelte",
-      "vite/client"
+      "vite/client",
+      "node"
     ],
     "moduleDetection": "force",
     "allowJs": true
````

- [ ] **Step 2: Run them to see them fail**

Run: `DATA_AVALANCHE_UI=required DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_BROWSER=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 console_app::`
Expected: `oauth2_sign_in_in_a_browser` fails: after "Continue with", the browser stays on the provider's `/authorize`, which the test provider doesn't answer for a browser, so no `h2` "Overview" appears. The budget: with `/v1/pipelines` reading one pipeline after another, the list took 650–1 010 ms
on the local emulators and the page went past 1 000 ms in one run of four (it can't be told apart
from the machine's load in a single run, so it isn't the step's stated failure). On PostgreSQL with `compose.yaml`'s earlier limits (the server's defaults), the budget's pipelines don't all start: "sorry, too many clients already" (seen while writing the plan; the replay ran on the services already recreated).

- [ ] **Step 3: Implement**

Let the test provider answer a browser; let PostgreSQL take fifty pipelines (then `docker compose -f test-infra/compose.yaml up -d --force-recreate` the PostgreSQL services); and read `/v1/pipelines`' pipelines at once, bounded, since the first page waits on that list:

`crates/data-avalanche-api/src/pipelines.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/pipelines.rs b/crates/data-avalanche-api/src/pipelines.rs
index d3a4c95..4c8d0f6 100644
--- a/crates/data-avalanche-api/src/pipelines.rs
+++ b/crates/data-avalanche-api/src/pipelines.rs
@@ -17,7 +17,7 @@ use data_avalanche_state::{
 };
 use serde_json::{Value, json};
 
-use crate::{Api, Failure, Node, Principal, body, starting};
+use crate::{Api, Failure, Node, Principal, READ_AT_ONCE, body, starting};
 
 /// What a status read failed on: 503, the store is the answer.
 pub(crate) fn unavailable(e: &StateError) -> Response {
@@ -142,8 +142,10 @@ fn summary(p: &Pipeline, r: &Read, members: &Members) -> Value {
     })
 }
 
-/// Every pipeline of the generation the node runs, by id.
+/// Every pipeline of the generation the node runs, by id, read [`READ_AT_ONCE`] at a time: the
+/// console's first page waits on this list (Admin_Console §8).
 pub(crate) async fn list(State(api): State<Api>) -> Response {
+    use futures_util::StreamExt;
     let Some(node) = api.node.get() else {
         return starting();
     };
@@ -151,19 +153,31 @@ pub(crate) async fn list(State(api): State<Api>) -> Response {
     let mut pipelines: Vec<&Pipeline> = config.pipelines.values().collect();
     pipelines.sort_by(|a, b| a.id.cmp(&b.id));
     let members = node.members.borrow().clone();
-    let mut out = vec![];
+    let mut each = vec![];
     for p in pipelines {
-        let docs = match (node.docs)(p) {
-            Ok(docs) => docs,
+        match (node.docs)(p) {
+            // Owned: a stream over references doesn't make a `Send` future (rust-lang/rust#64552).
+            Ok(docs) => each.push((p.clone(), docs)),
             Err(e) => {
                 return body(
                     StatusCode::INTERNAL_SERVER_ERROR,
                     &json!({ "error": format!("{}: the state location: {e}", p.id) }),
                 );
             }
-        };
-        match read(node, p, &docs).await {
-            Ok(r) => out.push(summary(p, &r, &members)),
+        }
+    }
+    let read = futures_util::stream::iter(each)
+        .map(|(p, docs)| async move {
+            let r = read(node, &p, &docs).await;
+            (p, r)
+        })
+        .buffered(READ_AT_ONCE)
+        .collect::<Vec<_>>()
+        .await;
+    let mut out = vec![];
+    for (p, r) in read {
+        match r {
+            Ok(r) => out.push(summary(&p, &r, &members)),
             Err(e) => return unavailable(&e),
         }
     }
````

`crates/data-avalanche/tests/e2e/idp.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/idp.rs b/crates/data-avalanche/tests/e2e/idp.rs
index dc80192..98b1731 100644
--- a/crates/data-avalanche/tests/e2e/idp.rs
+++ b/crates/data-avalanche/tests/e2e/idp.rs
@@ -212,13 +212,15 @@ impl Provider {
         let at = issuer.clone();
         let issued = Issued::default();
         let issued_in = std::sync::Arc::clone(&issued);
+        let codes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
+        let codes_in = std::sync::Arc::clone(&codes);
         let served = tokio::spawn(async move {
             let refreshes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
             loop {
                 let Ok((mut s, _)) = listener.accept().await else {
                     return;
                 };
-                let (jwks, discovery, key, roles, issuer, refreshes, issued) = (
+                let (jwks, discovery, key, roles, issuer, refreshes, issued, codes) = (
                     jwks.clone(),
                     discovery.clone(),
                     std::sync::Arc::clone(&key),
@@ -226,12 +228,23 @@ impl Provider {
                     at.clone(),
                     std::sync::Arc::clone(&refreshes),
                     std::sync::Arc::clone(&issued_in),
+                    std::sync::Arc::clone(&codes_in),
                 );
                 tokio::spawn(async move {
                     let Some((line, body)) = read_request(&mut s).await else {
                         return;
                     };
                     let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
+                    // A browser's sign-in: approved at once, sent back with a code.
+                    if path.starts_with("/authorize?") {
+                        let back = approval(&issuer, &issued, &codes, &format!("{issuer}{path}"));
+                        let answer = format!(
+                            "HTTP/1.1 302 Found\r\nLocation: {back}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
+                        );
+                        let _ = s.write_all(answer.as_bytes()).await;
+                        let _ = s.shutdown().await;
+                        return;
+                    }
                     let (code, answer) = if path.starts_with("/.well-known/openid-configuration") {
                         (200, discovery)
                     } else if path.starts_with("/jwks") {
@@ -308,7 +321,7 @@ impl Provider {
         Self {
             issuer,
             served,
-            codes: std::sync::Arc::default(),
+            codes,
             issued,
         }
     }
@@ -325,26 +338,37 @@ impl Provider {
     /// The provider's approval of the authorization request at `location`: where it sends the
     /// browser back, with a code and the request's state.
     pub fn approve(&self, location: &str) -> String {
-        assert!(
-            location.starts_with(&format!("{}/authorize?", self.issuer)),
-            "{location}"
-        );
-        assert_eq!(
-            query_value(location, "code_challenge_method").as_deref(),
-            Some("S256")
-        );
-        let back = query_value(location, "redirect_uri").expect("a redirect URI");
-        let state = query_value(location, "state").expect("a state");
-        let challenge = query_value(location, "code_challenge").expect("a code challenge");
-        let n = self.codes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
-        self.issued
-            .lock()
-            .unwrap()
-            .insert(format!("code-{n}"), (challenge, back.clone()));
-        format!("{back}?code=code-{n}&state={state}")
+        approval(&self.issuer, &self.issued, &self.codes, location)
     }
 }
 
+/// The approval of the authorization request at `location` by the provider at `issuer`: a new
+/// code, recorded with the request's challenge and redirect URI, and where the browser goes back.
+fn approval(
+    issuer: &str,
+    issued: &Issued,
+    codes: &std::sync::atomic::AtomicUsize,
+    location: &str,
+) -> String {
+    assert!(
+        location.starts_with(&format!("{issuer}/authorize?")),
+        "{location}"
+    );
+    assert_eq!(
+        query_value(location, "code_challenge_method").as_deref(),
+        Some("S256")
+    );
+    let back = query_value(location, "redirect_uri").expect("a redirect URI");
+    let state = query_value(location, "state").expect("a state");
+    let challenge = query_value(location, "code_challenge").expect("a code challenge");
+    let n = codes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
+    issued
+        .lock()
+        .unwrap()
+        .insert(format!("code-{n}"), (challenge, back.clone()));
+    format!("{back}?code=code-{n}&state={state}")
+}
+
 impl Drop for Provider {
     fn drop(&mut self) {
         self.served.abort();
````

`test-infra/compose.yaml`:

````diff
diff --git a/test-infra/compose.yaml b/test-infra/compose.yaml
index 6372f38..314c886 100644
--- a/test-infra/compose.yaml
+++ b/test-infra/compose.yaml
@@ -7,8 +7,9 @@ name: data-avalanche-test
 
 x-pg: &pg
   environment: { POSTGRES_USER: avalanche, POSTGRES_PASSWORD: avalanche, POSTGRES_DB: avalanche }
-  command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=20",
-            "-c", "max_wal_senders=20", "-c", "logical_decoding_work_mem=64kB"]
+  command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=64",
+            "-c", "max_wal_senders=64", "-c", "max_connections=300",
+            "-c", "logical_decoding_work_mem=64kB"]
   healthcheck: { test: ["CMD-SHELL", "pg_isready -U avalanche -d avalanche"], interval: 2s, retries: 30 }
 
 x-mariadb: &mariadb
@@ -59,7 +60,7 @@ services:
     <<: *pg
     image: "postgres:18"
     profiles: [replica]
-    command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=20",
+    command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=64",
               "-c", "max_wal_senders=20", "-c", "logical_decoding_work_mem=64kB",
               "-c", "synchronized_standby_slots=standby_sync"]
     environment: { POSTGRES_USER: avalanche, POSTGRES_PASSWORD: avalanche, POSTGRES_DB: avalanche, STANDBY_SLOT: standby_sync }
@@ -71,7 +72,7 @@ services:
     image: "postgres:18"
     profiles: [replica]
     entrypoint: ["bash", "/postgres-replica.sh"]
-    command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=20",
+    command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=64",
               "-c", "max_wal_senders=20", "-c", "logical_decoding_work_mem=64kB",
               "-c", "sync_replication_slots=on", "-c", "hot_standby_feedback=on"]
     environment: { POSTGRES_USER: avalanche, POSTGRES_PASSWORD: avalanche, POSTGRES_DB: avalanche, PRIMARY: postgres-sync-primary, STANDBY_SLOT: standby_sync }
````

- [ ] **Step 4: Run the tests and the gates**

Run: `DATA_AVALANCHE_UI=required DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_BROWSER=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 console_app::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
(cd ui && npm run check && npm test && npm run build)
```

Expected: no output from the first three; every test passes; svelte-check reports 0 errors and
0 warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/pipelines.rs crates/data-avalanche/tests/e2e/console_app.rs crates/data-avalanche/tests/e2e/console_sessions.rs crates/data-avalanche/tests/e2e/idp.rs crates/data-avalanche/tests/e2e/main.rs test-infra/compose.yaml ui/e2e/basic.spec.ts ui/e2e/budget.spec.ts ui/e2e/common.ts ui/e2e/jwt.spec.ts ui/e2e/none.spec.ts ui/e2e/oauth2.spec.ts ui/e2e/off.spec.ts ui/package-lock.json ui/package.json ui/playwright.config.ts ui/tsconfig.json
git commit -m "test(e2e): the console in a browser — sign-in per mode, the Overview live, the console off, the budget; /v1/pipelines reads at once"
```

---

### Task 6: Shipped (the image, CI, the chart, the site)

**Files:**
- Modify: `.dockerignore`
- Modify: `.github/workflows/ci.yml`
- Modify: `crates/data-avalanche/tests/packaging.rs`
- Modify: `deploy/docker/Dockerfile`
- Modify: `deploy/helm/data-avalanche/README.md`
- Create: `deploy/helm/data-avalanche/ci/console-values.yaml`
- Modify: `deploy/helm/data-avalanche/templates/NOTES.txt`
- Modify: `deploy/helm/data-avalanche/values.yaml`
- Modify: `doc/v1.0/README.md`
- Modify: `docs/src/content/docs/deployment/console.md`
- Modify: `docs/src/content/docs/deployment/container.md`
- Modify: `docs/src/content/docs/deployment/kubernetes.md`
- Modify: `test-infra/README.md`

**Interfaces:**
- Consumes: Tasks 1–5.
- Produces: the image's `ui` stage; CI's check job (`DATA_AVALANCHE_UI=required`, the app checked,
  tested and built) and integration job (the app and Chromium, `DATA_AVALANCHE_BROWSER=1`);
  `deploy/helm/data-avalanche/ci/console-values.yaml`; `NOTES.txt`'s console address.

- [ ] **Step 1: Write the failing tests**

The test (`packaging.rs`): `the_examples_configs_validate_with_the_pod_s_environment` takes
`ci/console-values.yaml` too, with a session key and a client secret made at run time.

`crates/data-avalanche/tests/packaging.rs`:

````diff
diff --git a/crates/data-avalanche/tests/packaging.rs b/crates/data-avalanche/tests/packaging.rs
index f4fc2fe..69d78a6 100644
--- a/crates/data-avalanche/tests/packaging.rs
+++ b/crates/data-avalanche/tests/packaging.rs
@@ -90,8 +90,20 @@ fn block(values: &str, key: &str) -> String {
     text
 }
 
+/// 48 random bytes, as the console's session key: made for the test.
+fn session_key() -> String {
+    use base64::Engine;
+    use std::io::Read;
+    let mut b = [0u8; 48];
+    std::fs::File::open("/dev/urandom")
+        .and_then(|mut f| f.read_exact(&mut b))
+        .expect("the system's random source");
+    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
+}
+
 /// What the chart puts in every pod's environment, and the secrets the examples' configs name:
-/// a password, and the API's certificate and key as PEM text, made for the test.
+/// a password, the API's certificate and key as PEM text, and the console's session key and
+/// client secret, made for the test.
 fn pod_env() -> BTreeMap<String, String> {
     let tls = rcgen::generate_simple_self_signed(vec!["data-avalanche".into()]).unwrap();
     [
@@ -109,6 +121,8 @@ fn pod_env() -> BTreeMap<String, String> {
         ("ORDERS_DB_PASSWORD", "secret".to_owned()),
         ("API_TLS_CERT", tls.cert.pem()),
         ("API_TLS_KEY", tls.signing_key.serialize_pem()),
+        ("CONSOLE_SESSION_KEY", session_key()),
+        ("CONSOLE_CLIENT_SECRET", session_key()),
     ]
     .into_iter()
     .map(|(k, v)| (k.to_owned(), v))
@@ -117,7 +131,11 @@ fn pod_env() -> BTreeMap<String, String> {
 
 #[test]
 fn the_examples_configs_validate_with_the_pod_s_environment() {
-    for example in ["ci/example-values.yaml", "ci/tls-values.yaml"] {
+    for example in [
+        "ci/example-values.yaml",
+        "ci/tls-values.yaml",
+        "ci/console-values.yaml",
+    ] {
         let config = block(&chart(example), "config");
         let report = validate_str(&config, &pod_env());
         assert!(report.is_ok(), "{example}: {:#?}\n{config}", report.errors);
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche --test packaging`
Expected: `the_examples_configs_validate_with_the_pod_s_environment` fails: `ci/console-values.yaml: No such file or directory`; 9 others pass.

- [ ] **Step 3: Implement**

Build the app in the image and in CI, give the chart its console case and notes, and document it:

`.dockerignore`:

````diff
diff --git a/.dockerignore b/.dockerignore
index eba4859..60d8a14 100644
--- a/.dockerignore
+++ b/.dockerignore
@@ -5,4 +5,9 @@
 !rust-toolchain.toml
 !crates/
 !xtask/
+!ui/
 crates/**/target/
+ui/node_modules/
+ui/dist/
+ui/test-results/
+ui/playwright-report/
````

`.github/workflows/ci.yml`:

````diff
diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml
index 0a19982..47d2521 100644
--- a/.github/workflows/ci.yml
+++ b/.github/workflows/ci.yml
@@ -18,8 +18,23 @@ env:
 jobs:
   check:
     runs-on: ubuntu-latest
+    # The console's app (M15) is built first, and every build below embeds it.
+    env:
+      DATA_AVALANCHE_UI: required
     steps:
       - uses: actions/checkout@v7
+      - uses: actions/setup-node@v7
+        with:
+          node-version-file: ui/.nvmrc
+          cache: npm
+          cache-dependency-path: ui/package-lock.json
+      - name: Console app
+        working-directory: ui
+        run: |
+          npm ci
+          npm run check
+          npm test
+          npm run build
       # Reads rust-toolchain.toml (channel + components) and caches the build.
       - uses: actions-rust-lang/setup-rust-toolchain@v2
       - name: Format
@@ -55,6 +70,9 @@ jobs:
           done
           # The API's TLS turns the probes to HTTPS.
           [ "$(helm template ci "$chart" -f "$chart/ci/tls-values.yaml" | grep -c 'scheme: HTTPS')" = 3 ]
+          # The console on: the notes give its address.
+          helm install --dry-run=client ci "$chart" -f "$chart/ci/console-values.yaml" \
+            | grep -qx '  https://cdc.example.com/ui/'
       - name: Workflows
         run: docker run --rm -v "$PWD:/repo:ro" --workdir /repo rhysd/actionlint:1.7.12 -no-color
       # RustSec advisory audit of Cargo.lock. Non-blocking by design: a vulnerable dependency is
@@ -81,6 +99,9 @@ jobs:
     env:
       DATA_AVALANCHE_IT: "1"
       DATA_AVALANCHE_GCS_BUCKET: ${{ vars.DATA_AVALANCHE_GCS_BUCKET }}
+      # The console's app built into the binary, and its flows in a browser (M15).
+      DATA_AVALANCHE_UI: required
+      DATA_AVALANCHE_BROWSER: "1"
     steps:
       # The workspace's test build and then the E2E build don't fit on the runner's disk beside
       # its preinstalled SDKs, which nothing here uses.
@@ -118,6 +139,19 @@ jobs:
           fi
       # uv runs the DuckDB cross-read of the Delta sink (R-12, crates/data-avalanche-sink-delta).
       - uses: astral-sh/setup-uv@v10.3.3
+      # The console's app, built before the first cargo build embeds it, and Playwright's
+      # Chromium for its flows (crates/data-avalanche/tests/e2e/console_app.rs).
+      - uses: actions/setup-node@v7
+        with:
+          node-version-file: ui/.nvmrc
+          cache: npm
+          cache-dependency-path: ui/package-lock.json
+      - name: Console app and its browser
+        working-directory: ui
+        run: |
+          npm ci
+          npm run build
+          npx playwright install --with-deps chromium
       - name: Start test infrastructure
         run: test-infra/up.sh
       - name: Integration tests
````

`deploy/docker/Dockerfile`:

````diff
diff --git a/deploy/docker/Dockerfile b/deploy/docker/Dockerfile
index c95a6e7..ef1f8ef 100644
--- a/deploy/docker/Dockerfile
+++ b/deploy/docker/Dockerfile
@@ -1,11 +1,22 @@
 # syntax=docker/dockerfile:1.29
 # The data-avalanche image (roadmap §4 M11a): the release binary on distroless, as the
-# non-root user. Built from the repository root:
+# non-root user, the console's app built into it (M15). Built from the repository root:
 #   docker build -f deploy/docker/Dockerfile -t data-avalanche .
 
+# The console's app: its files, embedded by the API crate's build.
+FROM node:26.13.1-trixie-slim AS ui
+WORKDIR /src/ui
+COPY ui/package.json ui/package-lock.json ./
+RUN --mount=type=cache,target=/root/.npm,sharing=locked npm ci
+COPY ui/ ./
+RUN npm run build
+
 FROM rust:1.100.0-trixie AS build
 WORKDIR /src
 COPY . .
+COPY --from=ui /src/ui/dist ui/dist
+# The build refuses to go on without the app.
+ENV DATA_AVALANCHE_UI=required
 # The target directory is a cache across builds. Debug info is stripped, the symbols kept: a
 # panic's backtrace still names its functions.
 RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
````

`deploy/helm/data-avalanche/README.md`:

````diff
diff --git a/deploy/helm/data-avalanche/README.md b/deploy/helm/data-avalanche/README.md
index a015d82..65ce7f3 100644
--- a/deploy/helm/data-avalanche/README.md
+++ b/deploy/helm/data-avalanche/README.md
@@ -43,6 +43,16 @@ The kubelet updates a mounted ConfigMap after a delay (up to about a minute), so
 `POST /v1/config/reload`, or call `POST /v1/config/validate` first, which reports what a reload
 would change.
 
+### The web console
+
+The console is in the config (`globals.console`), so the chart has no switch of its own: with
+`enabled: true` every pod serves it under `/ui/` on the API's port, through the chart's Service,
+and the install's notes give its address. Its session key (and an OAuth2 client secret) reach the
+config through `env` from a Secret; the key must be the same on every pod. The chart has no
+Ingress: put the Service behind yours, and set `public_url` to the address people's browsers use
+(required with `auth: oauth2`, where the provider sends them back to it).
+`ci/console-values.yaml` is a complete example, with sign-in through an OpenID provider.
+
 ## Values
 
 | Key | Default | |
````

`deploy/helm/data-avalanche/ci/console-values.yaml`:

````diff
diff --git a/deploy/helm/data-avalanche/ci/console-values.yaml b/deploy/helm/data-avalanche/ci/console-values.yaml
new file mode 100644
index 0000000..266274e
--- /dev/null
+++ b/deploy/helm/data-avalanche/ci/console-values.yaml
@@ -0,0 +1,45 @@
+# The cluster with the web console on (Admin_Console §11): people sign in through the company's
+# OpenID provider, and the console is served under /ui/ on the API's Service. The session key and
+# the client secret come from a Secret; the session key is the same on every pod. CI renders this
+# file and checks NOTES.txt gives the console's address; packaging.rs validates its config.
+replicaCount: 3
+env:
+  - name: ORDERS_DB_PASSWORD
+    valueFrom:
+      secretKeyRef: { name: orders-db, key: password }
+  - name: CONSOLE_SESSION_KEY
+    valueFrom:
+      secretKeyRef: { name: data-avalanche-console, key: session-key }
+  - name: CONSOLE_CLIENT_SECRET
+    valueFrom:
+      secretKeyRef: { name: data-avalanche-console, key: client-secret }
+config:
+  config_version: 1
+  globals:
+    cloud: aws
+    state: { bucket: example-cdc-state, prefix: cdc }
+    api:
+      address: "0.0.0.0:8080"
+      auth: oauth2
+      oauth2:
+        issuer: https://login.example.com/realms/ops
+        discovery_url: https://login.example.com/realms/ops/.well-known/openid-configuration
+        audience: cdc
+        roles_claim: roles
+        client_id: cdc-console
+        client_secret: ${CONSOLE_CLIENT_SECRET}
+    console:
+      enabled: true
+      session_key: ${CONSOLE_SESSION_KEY}
+      # Where people's browsers reach the console: the provider sends them back here.
+      public_url: https://cdc.example.com
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
````

`deploy/helm/data-avalanche/templates/NOTES.txt`:

````diff
diff --git a/deploy/helm/data-avalanche/templates/NOTES.txt b/deploy/helm/data-avalanche/templates/NOTES.txt
index 0c376cd..3464795 100644
--- a/deploy/helm/data-avalanche/templates/NOTES.txt
+++ b/deploy/helm/data-avalanche/templates/NOTES.txt
@@ -8,6 +8,18 @@ The API, /metrics and the probes: {{ include "data-avalanche.fullname" . }}.{{ .
   curl http://localhost:{{ include "data-avalanche.apiPort" . }}/v1/cluster/status
   {{- end }}
 When the config turns the API's auth on, the request needs its credentials.
+{{- $config := include "data-avalanche.configMap.values" . | fromYaml }}
+{{- if dig "globals" "console" "enabled" false $config }}
+
+The web console:
+{{- with dig "globals" "console" "public_url" "" $config }}
+  {{ . }}/ui/
+where people's browsers reach the API's Service (globals.console.public_url).
+{{- else }}
+  {{ lower (include "data-avalanche.scheme" .) }}://localhost:{{ include "data-avalanche.apiPort" . }}/ui/
+through the port-forward above.
+{{- end }}
+{{- end }}
 
 A changed config reaches the running cluster through the API's reload (POST /v1/config/reload),
 not through a restart: a starting node adopts the cluster's latest config generation.
````

`deploy/helm/data-avalanche/values.yaml`:

````diff
diff --git a/deploy/helm/data-avalanche/values.yaml b/deploy/helm/data-avalanche/values.yaml
index b8e5355..706ef4b 100644
--- a/deploy/helm/data-avalanche/values.yaml
+++ b/deploy/helm/data-avalanche/values.yaml
@@ -17,6 +17,13 @@
 #   globals:
 #     api:
 #       advertise_url: https://${DATA_AVALANCHE_POD_DNS_NAME}:8443
+# and, with the web console on (served under /ui/ on the API's Service; ci/console-values.yaml),
+# its session key the same on every pod, from a Secret through env:
+#   globals:
+#     console:
+#       enabled: true
+#       session_key: ${CONSOLE_SESSION_KEY}
+#       public_url: https://cdc.example.com    # with auth oauth2: where browsers reach it
 # A changed config reaches a running cluster through the API's reload, not a pod restart: a
 # starting node adopts the cluster's latest config generation, not its file.
 config: {}
````

`doc/v1.0/README.md`:

````diff
diff --git a/doc/v1.0/README.md b/doc/v1.0/README.md
index 0f9e282..8fda8ce 100644
--- a/doc/v1.0/README.md
+++ b/doc/v1.0/README.md
@@ -73,6 +73,8 @@ This folder holds the plan for **building** v1. The design it implements is in `
 | `DATA_AVALANCHE_PG_REPLICA=<primary port>:<replica port>` (from M12a) | The PostgreSQL failover E2E scenario on `test-infra`'s `replica` profile (`55420:55421`); unset, it is skipped |
 | `DATA_AVALANCHE_E2E_STORE=rustfs\|azurite\|s3\|gcs\|azure` (from M12c) | The store of an E2E run's state and lake (default `rustfs`); the cloud subset (`test-infra/cloud-e2e.txt`) on a real cloud |
 | `DATA_AVALANCHE_LOAD` (non-empty, from M12c) | Enables the acceptance runs (`--test load`), with `DATA_AVALANCHE_IT`; `cargo xtask bench\|chaos\|soak` sets both |
+| `DATA_AVALANCHE_UI=required` (from 1.1.0 M15) | The API crate's build fails unless `ui/dist` holds the console's app (`npm ci && npm run build` in `ui/`); unset, a build without it serves the "isn't built" page. CI and the image set it |
+| `DATA_AVALANCHE_BROWSER` (non-empty, from 1.1.0 M15) | The console's flows in Chromium (`console_app.rs`, Playwright from `ui/`), with `DATA_AVALANCHE_IT`; needs the app built in and `npx playwright install chromium` in `ui/` |
 
 CI secrets and variables are listed in `test-infra/README.md` ("CI requirements") and in the comments
 of `.github/workflows/ci.yml`.
````

`docs/src/content/docs/deployment/console.md`:

````diff
diff --git a/docs/src/content/docs/deployment/console.md b/docs/src/content/docs/deployment/console.md
index 31a8ced..b709db5 100644
--- a/docs/src/content/docs/deployment/console.md
+++ b/docs/src/content/docs/deployment/console.md
@@ -1,6 +1,6 @@
 ---
-title: Web console sign-in
-description: Switching the web console on, how people sign in under each API auth mode, and what a session is.
+title: Web console
+description: Switching the web console on, what it shows, how people sign in under each API auth mode, and what a session is.
 sidebar:
   order: 5
 ---
@@ -26,6 +26,40 @@ everyone out. Make one with `openssl rand -base64 48`.
 Serve the console over HTTPS, on the listener (`globals.api.tls`) or on a proxy in front of it: its
 cookies are `Secure`, which browsers send over plain HTTP only to `localhost`.
 
+## What it shows
+
+Every page has the same top bar: the sections, the live connection's state, the cluster's config
+generation, and who is signed in, with their role and the sign-out. This release has the
+**Overview**; the other sections say they aren't in this build yet.
+
+The page shows the cluster as it is when it opens, then follows the cluster's live updates
+(`GET /v1/stream`) through whichever node served it, so it changes as things happen without a
+reload. The bar says `live`, `reconnecting` with how long since the last update, or, after 30 s
+without one, the page says it is offline since then and that everything shown is as of that time,
+with a way to retry at once. The connection comes back by itself.
+
+The **Overview** answers "is anything wrong?":
+
+- tiles: the pipelines by state, the worst lag against its target, the nodes up and their config
+  generation, the snapshots running (and failed today), and the WAL the PostgreSQL slots hold;
+- **Needs attention**: what is wrong now, red first, then orange, then yellow, the newest first,
+  each with a link to where it's dealt with;
+- every pipeline, the worst first, with its state, node, lag over the last hour against the target,
+  and last commit;
+- the newest events.
+
+What needs attention, and the thresholds (fixed in this release, and stated at the foot of the
+page; a later release makes them configurable):
+
+| | |
+|---|---|
+| Red | A table halted; a pipeline stopped by an error; a pipeline no node has held for over 30 s; a node the others' probes get no answer from |
+| Orange | A pipeline's lag over 60 s and higher than 5 min ago; a slot holding WAL past 50% of `max_slot_wal_keep_size` (16 GB when the server sets none); a lease lost or taken over in the last hour; a snapshot job failed in the last 24 h; a node the console can't follow; a pipeline that can't commit |
+| Yellow | A node on another config generation; a restart-only change waiting on a node; a pipeline paused or stopped by a person over 1 h ago |
+
+A node the console can't follow shows its pipelines as last known, marked with how long it has been
+out of reach.
+
 ## Signing in
 
 | `globals.api.auth` | What a person sees |
@@ -142,3 +176,16 @@ ends with its session, with a `reset` message, and the page opens it again.
   by default: raise it in front of the console.
 - Without `Authorization`, an API 401 carries `WWW-Authenticate: Session realm="data-avalanche"`
   while the console's sessions are on, so a browser shows no credentials popup. Pass it through.
+
+## Building it from source
+
+The console is built into the binary: the release images carry it. A binary built from a checkout
+carries it when `ui/` was built first, with Node.js (the version in `ui/.nvmrc`):
+
+```sh
+cd ui && npm ci && npm run build && cd ..
+cargo build --release -p data-avalanche
+```
+
+Without that, the console's pages say the app isn't built into this binary, and the sign-in and
+the API work as before. Set `DATA_AVALANCHE_UI=required` to make the build fail instead.
````

`docs/src/content/docs/deployment/container.md`:

````diff
diff --git a/docs/src/content/docs/deployment/container.md b/docs/src/content/docs/deployment/container.md
index 940dabf..0c3bc1a 100644
--- a/docs/src/content/docs/deployment/container.md
+++ b/docs/src/content/docs/deployment/container.md
@@ -29,6 +29,9 @@ To build it from a checkout instead:
 docker build -f deploy/docker/Dockerfile -t data-avalanche:dev .
 ```
 
+The image's build makes the [web console](/data-avalanche/deployment/console/) in a Node.js stage first,
+and the binary's build refuses to go on without it.
+
 ## The command line
 
 ```text
````

`docs/src/content/docs/deployment/kubernetes.md`:

````diff
diff --git a/docs/src/content/docs/deployment/kubernetes.md b/docs/src/content/docs/deployment/kubernetes.md
index cdb4575..d60eaba 100644
--- a/docs/src/content/docs/deployment/kubernetes.md
+++ b/docs/src/content/docs/deployment/kubernetes.md
@@ -121,3 +121,28 @@ names after the release (after the release alone when its name already contains
 the part before `-headless` is cut at 52 characters, so a long release name is shortened there:
 check it with `kubectl get service`). A node trusts the system's roots and every certificate in its
 own `api.tls.cert`: put your CA after the node's certificate there when it is a private one.
+
+## The web console
+
+The [web console](/data-avalanche/deployment/console/) is switched on in the config, so the chart
+has no setting for it: with `globals.console.enabled: true` every pod serves it under `/ui/` on the
+API's port, through the chart's Service, and the install's notes give its address. Its session key
+must be the same on every pod: pass it, and an OAuth2 client secret, from a Secret through `env`:
+
+```yaml
+env:
+  - name: CONSOLE_SESSION_KEY
+    valueFrom:
+      secretKeyRef: { name: data-avalanche-console, key: session-key }
+config:
+  globals:
+    console:
+      enabled: true
+      session_key: ${CONSOLE_SESSION_KEY}
+      public_url: https://cdc.example.com    # with auth: oauth2
+```
+
+The chart has no Ingress: put the Service behind yours, at the root of one host
+([Behind a proxy](/data-avalanche/deployment/console/#behind-a-proxy)), and set `public_url` to the
+address people's browsers use. `ci/console-values.yaml` in the chart's directory is a complete
+example, with sign-in through an OpenID provider.
````

`test-infra/README.md`:

````diff
diff --git a/test-infra/README.md b/test-infra/README.md
index c527ac4..77fd647 100644
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -87,6 +87,17 @@ DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_E2E_SOURCE=mysql DA
   cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1
 ```
 
+The console's flows in a browser (`console_app.rs`, 1.1.0 M15) run the app's Playwright specs
+(`ui/e2e/`) against the suite's nodes, when `DATA_AVALANCHE_BROWSER` is set and the binary carries
+the app. The first-page budget runs 50 pipelines on PostgreSQL, so the PostgreSQL services allow
+64 slots and WAL senders and 300 connections:
+
+```sh
+(cd ui && npm ci && npm run build && npx playwright install chromium)
+DATA_AVALANCHE_UI=required DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_BROWSER=1 \
+  cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 console_app::
+```
+
 On MySQL each test makes a database and a user named `e2e_<test>` (the user's binlog dump shows
 the stream is open). Two scenarios change server-wide state: the purge scenarios purge the
 target's older binlogs, and the failover scenario (on `mysql-84-nogtid`) makes its database and
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche --test packaging`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
(cd ui && npm run check && npm test && npm run build)
```

Expected: no output from the first three; every test passes; svelte-check reports 0 errors and
0 warnings.

- [ ] **Step 5: Commit**

```bash
git add .dockerignore .github/workflows/ci.yml crates/data-avalanche/tests/packaging.rs deploy/docker/Dockerfile deploy/helm/data-avalanche/README.md deploy/helm/data-avalanche/ci/console-values.yaml deploy/helm/data-avalanche/templates/NOTES.txt deploy/helm/data-avalanche/values.yaml doc/v1.0/README.md docs/src/content/docs/deployment/console.md docs/src/content/docs/deployment/container.md docs/src/content/docs/deployment/kubernetes.md test-infra/README.md
git commit -m "build: the console shipped — the image builds the app, CI runs it, the chart's console values and notes"
```

---

## Exit

- Every task committed, one commit each, after the plan's commits; the gates pass on the last one
  (`cargo fmt --all --check`, both clippy runs, `cargo test --workspace`, and in `ui/`
  `npm run check`, `npm test`, `npm run build`).
- With the emulators up, the app built in and Chromium installed: the whole E2E suite on
  PostgreSQL 18 with `DATA_AVALANCHE_UI=required DATA_AVALANCHE_BROWSER=1`, and the console's
  flows among it.
- The image builds (`docker build -f deploy/docker/Dockerfile .`); the chart's checks pass on
  every `ci/*-values.yaml` (`helm lint --strict`, `helm template` through kubeconform), and the
  console case's notes give `https://cdc.example.com/ui/`; actionlint passes.
- `cargo xtask docs --check` and `cd docs && npm run build` pass.
- At the plan's verification (the code above, on `125438c`, each task's red and green commits
  apart, the diffs replayed to the same trees): every Step 2 failure as stated (Task 5's budget
  part as noted there); at Task 6, `cargo test --workspace` 1853 passed, both clippy runs and
  `cargo fmt --all --check` clean, in `ui/` svelte-check 0 errors and 0 warnings and 39 tests
  passed; the whole E2E suite on PostgreSQL 18 with the browser flows passed (64 tests, 896 s;
  the first page drawn in 257 ms; 220–661 ms over five runs of the budget alone); the image built
  (its `ui` stage, then the binary with `DATA_AVALANCHE_UI=required`), on the tree before Task 4's
  and Task 5's last changes, which touch no file the image's build reads differently; `helm
  lint --strict` and kubeconform passed on the three `ci/*-values.yaml`, actionlint passed;
  `cargo xtask docs --check` held and the site built. The first full E2E run found two faults the
  plan now carries: the figures of a pipeline with no worker have no `source`, which stopped the
  page (Task 4), and `/v1/pipelines` read one pipeline after another (Task 5).
- Then, on Oleksii's request: the whole-branch review and its fixes, the push, the PR.

## Deviations during execution

None yet. Where execution changes the plan, this section says what and why.
