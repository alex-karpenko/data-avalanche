---
updated: 2026-11-25
domain: Architecture
tags: [v1.1, implementation-plan, m16a, console, web-ui, svelte, pipelines, actions, charts, uplot, playwright]
resolves: []
---

# M16a — The Console's Pipelines and Their Actions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The console's Pipelines section (Admin_Console §4, §5; mockups 05 and 06): the list, a
pipeline's page with its lag chart, and the pipeline actions with their dialog, one pipeline or
several; with the pieces every later page shares (the cluster's model, the chart, the dialog),
and the navigation budget measured in a browser.

**Architecture:**

- **The shared model** (Task 1, `ui/src/model/`): M15's Overview data becomes the app's one model
  of the cluster, which every page reads, so moving between pages reads nothing again. The list's
  rows name each pipeline's source and sink (`GET /v1/pipelines` adds them from the config).
- **The chart** (Task 2, `ui/src/lib/{series.ts,Chart.svelte}`): uPlot draws the line, the axes
  and the crosshair; the page draws the target, the events as dotted lines and their labels
  (stacked, hover-linked to the page's list). uPlot is loaded with the first chart, in its own
  file, so the Overview never loads it.
- **The actions** (Task 3, `ui/src/lib/{actions.ts,Dialog.svelte}`): each pipeline action's
  dialog says what it does, to what and its effect, takes a note, and names the line it will be
  recorded as; several pipelines are asked one at a time, stopping at the first failure.
- **The list** (Task 4, `ui/src/pipelines/{rows.ts,lag.svelte.ts,List.svelte}`): worst first,
  filters and sort in the address, the keyboard, a selection to act on, the lag over 6 hours.
- **A pipeline's page** (Task 5, `ui/src/pipelines/{detail.ts,page.svelte.ts,Pipeline.svelte}`):
  drawn at once from what the console knows, then each part read on its own and followed through
  the stream: header, actions, lag chart with its events, tables, source, snapshot, activity.
- **In a browser** (Task 6): the pipelines' flows, a viewer's, and the navigation budget, in
  Playwright under the E2E suite; the site's console page.

**Tech Stack:** Svelte 5.59.1, TypeScript 6.0.3, Vite 8.3.8, Vitest 5.1.1 with jsdom 31.0.1,
svelte-check 4.7.8, `@playwright/test` 1.65.1 (Chromium), all as M15 left them; **uPlot 1.6.32**,
the app's first runtime package (exact, tracked by `xtask deps`). Rust 1.100.0, no new crate.

**Spec:**
- `doc/research/Admin_Console.md` §4 (the Pipelines row; lists: a row opens its page, real links,
  filters and sort in the address, Back keeps them, ↑/↓ and Enter), §4.2 (charts), §5 (actions,
  the dialog, attribution), §8 (now first, the age of what's shown, the 300 ms budget), §10.
- The approved mockups `doc/v1.1/mockups/05-pipelines.html` and `06-pipeline.html`; `11-…`
  (actions disabled offline, a node out of reach).
- `doc/v1.1/M15-console-overview.md` (the frame, the model, the browser flows),
  `M13d-console-pipelines.md` (`/v1/pipelines/{id}/live`, the tables' figures),
  `M13e-console-history.md` (`/v1/history`), `M13c-console-jobs.md` (the jobs),
  `M13a-console-events.md` (`/v1/events`, the `note`), `M13f-console-stream.md`.
- `doc/specs/Configuration_Specification.md` §12.4 (a pipeline's restart-only change restarts it
  at reload); no spec change.

## Global Constraints

- Toolchain `1.100.0`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`;
  - in `ui/`: `npm run check` (svelte-check, warnings fail it), `npm test`, `npm run build`.
- **Every release refreshes its dependencies** (CLAUDE.md): `uplot` is an exact version, the
  newest stable one (1.6.32); `xtask deps` reads `ui/package.json`'s `dependencies` and
  `devDependencies`.
- **Under the console's Content-Security-Policy** (M14: `script-src 'self'; style-src 'self'`): no
  inline script, no `style` attribute in markup. A position the page computes is set through
  Svelte's `style:` directive (the CSSOM, which the policy allows), as uPlot sets its own.
- **Every action opens one dialog** (Admin_Console §5): what it does, to what, its effect; an
  optional note (at most 500 characters, the server's `MAX_NOTE`); the line it will be recorded
  as. Only an `operator` or an `admin` sees an action; the server checks the role again and
  records the action whatever the page shows. Every call that changes something goes through
  M15's client (`X-CSRF-Token`).
- **Several pipelines are asked one at a time, in order, and a failure stops the rest** (mockup
  05), saying which failed, why, and which weren't asked.
- **Now first, history second; age always shown** (Admin_Console §8): a page draws what the
  console knows at once; each part is read on its own and keeps what it had when a read fails,
  saying so; while the stream is stale every panel says how old it is; while offline actions
  wait.
- **Page budgets** (Admin_Console §8), with 50 pipelines on three nodes: the first page within
  1 s (M15) and moving between pages within **300 ms** (Task 6).
- **No two files whose names differ only by case** (`list.ts` and `List.svelte`'s test would be one
  file on macOS): a component's pure logic lives in a file of another name.
- **Test gates:** plain `cargo test` and `npm test` stay hermetic. The browser flows need
  `DATA_AVALANCHE_IT=1`, `DATA_AVALANCHE_BROWSER=1`, the app built into the binary and
  Playwright's Chromium; they run in the E2E suite with `--test-threads=1`.
- **The site follows the behaviour in the same milestone** (`doc/v1.0/README.md`).
- **Code blocks:**
  - They are the verified content, already formatted (`rustfmt`; the TypeScript and Svelte as
    written, checked by svelte-check).
  - Every block is a `git apply`-ready diff, one per file; a moved file's is a rename diff.
  - A step's diffs apply in the order given, after the earlier steps'.
  - The regenerated files are written by their commands, not by hand: `ui/package-lock.json` by
    `npm install` (Task 2), `crates/data-avalanche-api/openapi.json` by
    `DATA_AVALANCHE_BLESS=1 cargo test -p data-avalanche-api openapi`, and
    `docs/public/openapi.json` and `docs/src/content/docs/reference/api.md` by `cargo xtask docs`
    (Task 1).
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to hurt someone running 1.1.0 in production. Each is pinned by a
test in the task after the arrow.

1. **An action on several pipelines where one fails** (a 409: no node holds its lease, or it was
   paused meanwhile): the rest must not be asked, and the person must learn which one failed, why,
   and which weren't asked → Task 3 (`asks one at a time, in order, and stops at the first
   failure, saying which`), Task 4 (`acts on several, one at a time, naming those the action
   skips, and stops at the first failure`).
2. **An action pressed while the console is offline**: nothing may be sent on numbers the page
   says are old → Task 3 (`can't be confirmed while blocked, and says why`), Task 4 (`keeps
   actions from being confirmed while offline`), Task 5 (`keeps its actions from being taken while
   offline`).
3. **A pipeline whose lease no node holds** (stopped, starting, mid-failover): its page must still
   draw what storage knows, say no node holds it, and say which read failed → Task 5 (`heads with
   its list row's alone until its own read, and says when no node holds it`, `lists the halts
   storage keeps while no worker shows its tables`, `says a pipeline the generation lacks is
   missing, and keeps a part that fails to read`).
4. **A failover while a page is open**: the old owner's figures arriving after the new owner's
   must not overwrite them → Task 5 (`takes its own figures from the stream, the newest owner's,
   and no other pipeline's`).
5. **A link someone shared, or an address edited by hand**: the same view, and a value the page
   doesn't know is the default, not a broken page → Task 4 (`keep their view in the address, read
   back as written`, `shows the view its address names, and changes the address in place`).

## Decisions and rulings

Settled with Oleksii before this plan (2026-11-25):
- M16 is split in four milestones, one branch and one PR each: **M16a** the shared pieces (the
  model, the chart, the dialog, lists with their address and keys), the Pipelines list and page
  with their actions, and the navigation budget; **M16b** the Cluster and a node's page, node
  actions, DA-API-21 (a route for pending removals, then the yellow rule) and the console's
  status; **M16c** Tables and a table's page, Snapshots and a job's page, job actions; **M16d**
  Events (histogram, export) and Config (four tabs, the diff, validate and reload), and the
  Overview's actions read past the events' scan limit.
- **uPlot** draws the time series (Admin_Console §10).

The rest are Claude's calls.

- **D-M16a.1 The shared model** (`ui/src/model/`). M15's `overview/{types,facts,model.svelte,
  fixtures}.ts` move there and the class `Overview` becomes `Model`: one for the app, read at its
  start and followed through the stream, so a page reached by a link reads nothing it shares with
  the others (the navigation budget). `PipelineItem` gains `source` and `sink` (the config's), which
  `GET /v1/pipelines` now sends; `route(p, long)` writes them (`PG orders@pg-prod → Δ lake/orders`,
  or the engines written out on a pipeline's page). The tables' and jobs' types gain the fields
  the server sends that these pages show.
- **D-M16a.2 The chart** (`series.ts`, `Chart.svelte`). uPlot (dynamically imported, its own
  51 kB file) draws the line, the time axis, the value axis in the series' unit and the crosshair;
  its `draw` hook draws the target dashed and each event dotted (`drawOverlay`, the hovered one
  wider, in the accent colour); the labels are buttons in the page's small text above the plot,
  stacked on up to `LABEL_ROWS` (3) lines when they would overlap (`placeLabels`). Hovering a label,
  or the plot within `HOVER_PX` (4) of a line, calls `onHover(key)`; the page highlights that event
  in its list and passes `hovered` back. The readout says `13:41:05 · 4 s` at the pointer, else the
  value now. A range without samples says so. Lag lines read 6 hours at a 60 s step (360 points).
- **D-M16a.3 The actions** (`actions.ts`). Flush, pause, resume, restart, stop, in that order.
  Each is offered in the desired states the server takes it in (else it answers 409): pause and
  restart while running, resume while paused or stopped, flush and stop unless stopped. The
  effects say what the server does: a restart **drops what isn't committed and reads it again**
  from the committed position (the API's restart discards the unflushed and replays; mockup 06
  says "flushes", which the server doesn't do); only stop is styled destructive. No action is
  irreversible, so none asks for a second confirmation (§5 asks for one only where an action can't
  be undone). Recorded as `restarted by Oleksii (operator)`, `, on each pipeline` for several.
  The note is trimmed; an empty one sends no body.
- **D-M16a.4 The dialog** (`Dialog.svelte`). One component for every action: title, a paragraph
  each for what and its effect, the action's own fields (a snippet), the note, `Recorded as:`,
  Cancel and the action. It focuses the note, closes on Escape or Cancel, stays open with the
  server's words when the action fails, and can't be confirmed while `blocked` (offline: "actions
  wait until the console is live again"; nothing chosen).
- **D-M16a.5 Several pipelines** (`oneAtATime`, `outcomeText`). The list's bar offers the actions
  at least one selected pipeline takes; the dialog names each it will ask with its node, the
  running snapshot a restart carries on, "one at a time …", and those the action doesn't apply to
  (`Not asked, restart doesn't apply: billing-my (paused)`). A failure ends the run:
  `ledger-my: <the server's words>. Asked already: crm-pg. Not asked: payments-pg.` What was asked
  leaves the selection.
- **D-M16a.6 The list** (`rows.ts`, `List.svelte`). The rows build on the Overview's (the same
  worst-first order and state chip) and add the route, `away` (a holder that isn't the ring owner),
  changes a minute (the live figures' tables' sum), tables (`13 of 14` with a halt), and the
  snapshot (the running job's `62% · ETA 7m 0s`, else the newest ended job's outcome). Filters:
  All, ⚠ Needs attention (the pipelines the Overview's attention lines link to), Running, Halted
  tables, Paused, Stopped, Snapshotting, with counts; node; source; name. A column's heading
  sorts by it (numbers biggest first, again for the other way). The view lives in the address
  (`?show=halted&node=n2&source=postgres&q=ord&sort=lag&desc`), changed in place
  (`history.replaceState`), so Back leaves the list and returns to it as it was; an unknown value
  is the default. A click anywhere on a row opens it (the id is the real link); ↑/↓ move between
  rows' links; Enter is the link's own. The selection's checkboxes are an operator's or an
  admin's.
- **D-M16a.7 A pipeline's page** (`detail.ts`, `page.svelte.ts`, `Pipeline.svelte`). Drawn at once
  from its list row and the figures the stream already sent; then `GET /v1/pipelines/{id}`, its
  `/live`, its `/snapshots` and `GET /v1/events?object={id}&limit=50`, each on its own, and its lag
  over 6 hours. The stream's `figures` (the higher `owner_epoch` wins), `pipeline`, `samples` and
  `event` follow it; an event reads again, once a second at most, the parts it changes. The header:
  state chip, route, `on n2 · away from owner n1 · lease epoch 7, expires in 8 s`. Tables halted
  first; a table's schema column is its halt, else its last `schema.*` event of the day, else its
  soft-dropped columns, else `unchanged`; each links to its table's page (M16c). Source: PostgreSQL's
  slot (inactive is orange, none red), WAL held against `max_slot_wal_keep_size` (orange past
  M15's threshold), confirmed LSN, publication; MySQL's position, binlogs kept, how far behind.
  Snapshot: the open job's progress, the three ended last, "all →". The chart's events: actions
  carried out, halts, failovers, lease losses, errors, snapshot jobs. Snapshot… asks for tables
  (the page's own) and truncate, and starts a job (`{"tables", "truncate", "note"}`). A 404 says
  the generation has no such pipeline.
- **D-M16a.8 Where the mockups and the server differ** (rulings):
  - *"restart pending" on a pipeline* (mockups 05, 06): the server never leaves a pipeline's
    change waiting: a restart-only pipeline field restarts the pipeline at reload (config §12.4);
    only `globals` fields wait for a node's restart (`restart_pending`). Neither page shows it;
    a node's waiting changes are Config's (M16d) and the Overview's (M15).
  - *"renews in 8 s"* (mockup 06): `GET /v1/cluster/status` gives when the lease ends unless
    renewed (`expires_at`); the header says `expires in`.
  - *The pipeline's resolved config* (§4's Pipelines row): mockup 06 has no such panel, and the
    mockup wins; Config (M16d) shows the running config.
  - *MariaDB* (mockup 05's `MDB`): the API names MySQL and MariaDB alike (`mysql`), so the list
    says `MY`.
- **D-M16a.9 The router** gains `rest` (the names after the section, decoded) and `query`, and
  `replace(to)`. `App.svelte` shows the list at `/ui/pipelines` and a pipeline's page at
  `/ui/pipelines/{id}`, its `PipelinePage` made when the id changes and dropped when it goes.
- **D-M16a.10 The navigation budget** (`budget.spec.ts`, `console_app.rs`). On the budget's cluster
  (50 pipelines, three nodes), once every pipeline runs on its ring owner (a pipeline being handed
  over has no tables to draw until its new worker runs: that would time the hand-off, not the
  page) and the stream has sent every pipeline's figures (the flow waits for all 50 on a stream of
  its own: a pipeline's page draws its tables from them, and reads them from the node that runs it
  while they haven't come), the page times in itself, from
  the click (or `history.back()`) to the first frame showing the target's rows: the Overview to
  the list (50 rows), the list to `p01` (its tables), Back to the list, the list to the Overview
  (its tiles). Each under 300 ms.
- **D-M16a.11 Rollback.** Nothing is stored. `GET /v1/pipelines` only gains fields.

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-api/src/{pipelines,lib}.rs`, `openapi.json`, `docs/…/reference/api.md`, `docs/public/openapi.json` | The list names source and sink (Task 1) |
| `ui/src/model/*` (moved from `ui/src/overview/`, `facts.test.ts` new), `ui/src/overview/*`, `ui/src/App.svelte` | The shared model (Task 1) |
| `ui/package.json`, `ui/package-lock.json`, `xtask/src/deps.rs`, `ui/src/lib/{series.ts,Chart.svelte}` (new) | The chart (Task 2) |
| `ui/src/lib/{actions.ts,Dialog.svelte}` (new) | The actions and their dialog (Task 3) |
| `ui/src/lib/router.svelte.ts`, `ui/src/pipelines/{rows.ts,lag.svelte.ts,List.svelte}` (new), `ui/src/model/{types,fixtures}.ts`, `ui/src/App.svelte` | The list (Task 4) |
| `ui/src/pipelines/{detail.ts,page.svelte.ts,Pipeline.svelte,fixtures.ts}` (new), `ui/src/model/types.ts`, `ui/src/App.svelte` | A pipeline's page (Task 5) |
| `ui/e2e/{pipelines.spec.ts (new),basic,jwt,budget}.spec.ts`, `crates/data-avalanche/tests/e2e/console_app.rs`, `docs/…/deployment/console.md` | In a browser; the site (Task 6) |

The plan's commit also carries `doc/v1.1/README.md` (M15 merged, #34; this plan's row and M16b–d's).
One more commit goes with it: `research: Admin_Console — M16's split; §4.3 the Pipelines as built in M16a`.

---

### Task 1: The shared model, and the list naming source and sink (`ui/src/model`, `/v1/pipelines`)

**Files:**
- Regenerate: `crates/data-avalanche-api/openapi.json`
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Modify: `crates/data-avalanche-api/src/pipelines.rs`
- Regenerate: `docs/public/openapi.json`
- Regenerate: `docs/src/content/docs/reference/api.md`
- Modify: `ui/src/App.svelte`
- Create: `ui/src/model/facts.test.ts`
- Move: `ui/src/overview/facts.ts` → `ui/src/model/facts.ts`
- Move: `ui/src/overview/fixtures.ts` → `ui/src/model/fixtures.ts`
- Move: `ui/src/overview/model.svelte.ts` → `ui/src/model/model.svelte.ts`
- Move: `ui/src/overview/model.test.ts` → `ui/src/model/model.test.ts`
- Move: `ui/src/overview/types.ts` → `ui/src/model/types.ts`
- Modify: `ui/src/overview/Overview.svelte`
- Modify: `ui/src/overview/Overview.test.ts`
- Modify: `ui/src/overview/attention.test.ts`
- Modify: `ui/src/overview/attention.ts`
- Modify: `ui/src/overview/derive.test.ts`
- Modify: `ui/src/overview/derive.ts`

**Interfaces:**
- Consumes: M15's `ui/src/overview/{types,facts,model.svelte,fixtures}.ts` and `model.test.ts`.
- Produces (`ui/src/model/`, D-M16a.1): `class Model` (M15's `Overview`, unchanged otherwise:
  `facts`, `read`, `load(parts?)`, `follow(live)`, `PARTS`, `REREAD_AFTER_MS`, `PipelineMsg`,
  `SamplesMsg`, `PeerMsg`, `Read`); `facts.ts`'s `route(p: { source: SourceAt; sink: SinkAt },
  long = false): string`; `types.ts`'s `SourceAt { type: 'postgres' | 'mysql'; host; port; db }`,
  `SinkAt { type: 'delta' | 'iceberg'; bucket; prefix }`, `PipelineItem.source`/`.sink`,
  `ClusterStatus.pipelines[].expires_at`/`.ring_owner`; `fixtures.ts`'s `facts()` (orders-pg held
  by n2 away from its ring owner n1).
- Produces (`GET /v1/pipelines`): each row's `source` (`type`, `host`, `port`, `db`) and `sink`
  (`type`, `bucket`, `prefix`), as `GET /v1/pipelines/{id}` already sent them.

- [ ] **Step 1: Write the failing tests**

The model's files move first (`git mv`), and the Overview's imports follow them; the fixtures
gain the new fields. The tests: `a pipeline's route` (`facts.test.ts`: short in a list, written
out on its page; the lake path without the prefix's slashes); `the_list_has_every_pipeline_s_summary`
expects `source` and `sink`.

`crates/data-avalanche-api/src/pipelines.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/pipelines.rs b/crates/data-avalanche-api/src/pipelines.rs
index 4c8d0f6fadd46e62ff2d472025235b85de3c7e08..6be7f586740f10a90b7f985aaa379c03d1cd9bd3 100644
--- a/crates/data-avalanche-api/src/pipelines.rs
+++ b/crates/data-avalanche-api/src/pipelines.rs
@@ -674,6 +674,8 @@ mod tests {
             json!({ "pipelines": [{
                 "id": "orders", "name": "orders", "desired_state": "running",
                 "actual_state": "paused", "holder": "node-b", "holder_stale": true,
+                "source": { "type": "postgres", "host": "db.example.com", "port": 5432, "db": "shop" },
+                "sink": { "type": "delta", "bucket": "lake", "prefix": "shop" },
             }] })
         );
     }
````

`ui/src/App.svelte`:

````diff
diff --git a/ui/src/App.svelte b/ui/src/App.svelte
index 6362cdb0aed08013a2c2eb4bddeaaa6c71d9f174..d51c1870ab4e056570c69257066ec092efba646d 100644
--- a/ui/src/App.svelte
+++ b/ui/src/App.svelte
@@ -7,7 +7,7 @@
   import { Live } from './lib/live.svelte';
   import { Router } from './lib/router.svelte';
   import { awaitSession, checkSession, keepFresh, RETRY_MS, type Session } from './lib/session';
-  import { Overview as OverviewModel } from './overview/model.svelte';
+  import { Model } from './model/model.svelte';
   import Overview from './overview/Overview.svelte';
 
   // The server says why the session ended: its 401 or 403 page, or the sign-in.
@@ -25,7 +25,7 @@
     open: (url) => new EventSource(url),
     check: () => checkSession({ fetch, ended }),
   });
-  const overview = new OverviewModel(api);
+  const model = new Model(api);
 
   async function readGeneration() {
     try {
@@ -42,7 +42,7 @@
       live.on('event', (e) => {
         if ((e as { kind?: string }).kind === 'config.published') void readGeneration();
       }),
-      overview.follow(live),
+      model.follow(live),
     ];
     const read = (s: Session) => {
       session = s;
@@ -50,7 +50,7 @@
       stops.push(keepFresh(s, { fetch, ended }, (next) => (session = next)));
       live.start();
       void readGeneration();
-      void overview.load();
+      void model.load();
     };
     stops.push(awaitSession({ fetch, ended }, read, (why) => (unread = why)));
     const tick = setInterval(() => {
@@ -76,7 +76,7 @@
     onRetry={() => live.retry()}
   >
     {#if router.section === 'overview'}
-      <Overview model={overview} {now} stale={live.stale(now)} last={live.last} />
+      <Overview {model} {now} stale={live.stale(now)} last={live.last} />
     {:else}
       <Later section={router.section} />
     {/if}
````

`ui/src/model/facts.test.ts`:

````diff
diff --git a/ui/src/model/facts.test.ts b/ui/src/model/facts.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..4d10d55ccd8caf995a9c232425c225c31b39ea05
--- /dev/null
+++ b/ui/src/model/facts.test.ts
@@ -0,0 +1,18 @@
+import { describe, expect, it } from 'vitest';
+import { route } from './facts';
+import { facts } from './fixtures';
+
+describe('a pipeline\'s route', () => {
+  it('names the source and the sink, short in a list and written out on its page', () => {
+    const [orders, billing] = facts().pipelines;
+    expect(route(orders)).toBe('PG orders@pg-prod-replica → Δ lake/orders-pg');
+    expect(route(billing)).toBe('MY billing@mysql-8 → Δ lake/billing-mysql');
+    expect(route(orders, true)).toBe('PostgreSQL orders@pg-prod-replica → Delta lake/orders-pg');
+  });
+
+  it('writes the lake path without the prefix\'s slashes', () => {
+    const p = facts().pipelines[0];
+    expect(route({ ...p, sink: { ...p.sink, prefix: '/orders/v2/' } })).toBe('PG orders@pg-prod-replica → Δ lake/orders/v2');
+    expect(route({ ...p, sink: { ...p.sink, prefix: '' } })).toBe('PG orders@pg-prod-replica → Δ lake');
+  });
+});
````

`ui/src/overview/facts.ts` → `ui/src/model/facts.ts`:

````diff
diff --git a/ui/src/overview/facts.ts b/ui/src/model/facts.ts
similarity index 96%
rename from ui/src/overview/facts.ts
rename to ui/src/model/facts.ts
index 3007d93b054e50345055d0235f8a313186865999..312c2e780a59828f913cc915cc941678770ca72f 100644
--- a/ui/src/overview/facts.ts
+++ b/ui/src/model/facts.ts
@@ -1,4 +1,4 @@
-// What the Overview knows, from its reads and the stream, and what follows from it for one
+// What the console knows, from its reads and the stream, and what follows from it for one
 // pipeline.
 
 import type {
````

`ui/src/overview/fixtures.ts` → `ui/src/model/fixtures.ts`:

````diff
diff --git a/ui/src/overview/fixtures.ts b/ui/src/model/fixtures.ts
similarity index 78%
rename from ui/src/overview/fixtures.ts
rename to ui/src/model/fixtures.ts
index 66abb8f5e9b582cc9c0eb075f6334a340da59b47..6bbd6f0feace1489f38eba34e6034f2355b277de 100644
--- a/ui/src/overview/fixtures.ts
+++ b/ui/src/model/fixtures.ts
@@ -1,7 +1,7 @@
 // The tests' cluster: mockup 01's, in the API's shapes.
 
 import type { Facts } from './facts';
-import type { EventItem, Figures } from './types';
+import type { Actual, ClusterStatus, Desired, EventItem, Figures, PipelineItem, SourceAt } from './types';
 
 export const NOW = Date.UTC(2026, 10, 25, 14, 2, 0);
 const MIN = 60_000;
@@ -38,22 +38,42 @@ const pause = event({
 const line = (f: (ago: number) => number): [number, number][] =>
   Array.from({ length: 13 }, (_, i) => 60 - i * 5).map((ago) => [(NOW - ago * MIN) / 1000, f(ago)]);
 
+const pg = (db: string, host: string): SourceAt => ({ type: 'postgres', host, port: 5432, db });
+
+function item(id: string, desired: Desired, actual: Actual, holder: string, source: SourceAt): PipelineItem {
+  return {
+    id,
+    name: id,
+    desired_state: desired,
+    actual_state: actual,
+    holder,
+    holder_stale: false,
+    source,
+    sink: { type: 'delta', bucket: 'lake', prefix: id },
+  };
+}
+
+function lease(id: string, holder: string, owner_epoch: number, ring_owner: string): ClusterStatus['pipelines'][number] {
+  return { id, holder, holder_stale: false, owner_epoch, expires_at: iso(NOW + 8_000), ring_owner };
+}
+
 export function facts(): Facts {
   return {
     pipelines: [
-      { id: 'orders-pg', name: 'orders-pg', desired_state: 'running', actual_state: 'running', holder: 'n2', holder_stale: false },
-      { id: 'billing-mysql', name: 'billing-mysql', desired_state: 'running', actual_state: 'running', holder: 'n1', holder_stale: false },
-      { id: 'inventory', name: 'inventory', desired_state: 'paused', actual_state: 'paused', holder: 'n3', holder_stale: false },
-      { id: 'customers', name: 'customers', desired_state: 'running', actual_state: 'running', holder: 'n1', holder_stale: false },
+      item('orders-pg', 'running', 'running', 'n2', pg('orders', 'pg-prod-replica')),
+      item('billing-mysql', 'running', 'running', 'n1', { type: 'mysql', host: 'mysql-8', port: 3306, db: 'billing' }),
+      item('inventory', 'paused', 'paused', 'n3', pg('inventory', 'pg-prod')),
+      item('customers', 'running', 'running', 'n1', pg('crm', 'pg-crm')),
     ],
     cluster: {
       node: 'n1',
       members: ['n1', 'n2', 'n3'].map((node) => ({ node, phase: 'active', suspect: false, config_generation: 42 })),
       pipelines: [
-        { id: 'orders-pg', holder: 'n2', holder_stale: false, owner_epoch: 2 },
-        { id: 'billing-mysql', holder: 'n1', holder_stale: false, owner_epoch: 3 },
-        { id: 'inventory', holder: 'n3', holder_stale: false, owner_epoch: 1 },
-        { id: 'customers', holder: 'n1', holder_stale: false, owner_epoch: 1 },
+        // Held by n2 away from its ring owner, since a failover.
+        lease('orders-pg', 'n2', 2, 'n1'),
+        lease('billing-mysql', 'n1', 3, 'n1'),
+        lease('inventory', 'n3', 1, 'n3'),
+        lease('customers', 'n1', 1, 'n1'),
       ],
     },
     config: {
````

`ui/src/overview/model.svelte.ts` → `ui/src/model/model.svelte.ts`:

````diff
diff --git a/ui/src/overview/model.svelte.ts b/ui/src/model/model.svelte.ts
similarity index 96%
rename from ui/src/overview/model.svelte.ts
rename to ui/src/model/model.svelte.ts
index 0fcee81e0ae58dd7c8102c5a10f4ad1970a1c787..2d3e2323ebd5450b12014d58c375480a56df7f57 100644
--- a/ui/src/overview/model.svelte.ts
+++ b/ui/src/model/model.svelte.ts
@@ -1,9 +1,10 @@
-// The Overview's data (Admin_Console §8): read at once, each part within its own time limit, then
-// kept current by the live stream. A part that can't be read keeps what it last had and says why.
+// What every page knows of the cluster (Admin_Console §8): read at once, each part within its own
+// time limit, then kept current by the live stream. A part that can't be read keeps what it last
+// had and says why. One for the app: moving between pages reads nothing again.
 
 import type { Client } from '../lib/api';
 import type { Live } from '../lib/live.svelte';
-import { LEASE_LOSS_WITHIN_S } from './attention';
+import { LEASE_LOSS_WITHIN_S } from '../overview/attention';
 import { empty, type Facts } from './facts';
 import type {
   Actual,
@@ -78,7 +79,7 @@ function partsOf(kind: string): Part[] {
   return [];
 }
 
-export class Overview {
+export class Model {
   facts = $state<Facts>(empty());
   read = $state<Record<Part, Read>>(
     Object.fromEntries(PARTS.map((p) => [p, { at: null, error: null }])) as Record<Part, Read>,
````

`ui/src/overview/model.test.ts` → `ui/src/model/model.test.ts`:

````diff
diff --git a/ui/src/overview/model.test.ts b/ui/src/model/model.test.ts
similarity index 93%
rename from ui/src/overview/model.test.ts
rename to ui/src/model/model.test.ts
index 95edae20ffc03472ccb1ec8acf250cba323d83a6..3ded598e429932e4460b2ded141a51c66fab6ff7 100644
--- a/ui/src/overview/model.test.ts
+++ b/ui/src/model/model.test.ts
@@ -1,7 +1,7 @@
 import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
 import { HttpError, type Client } from '../lib/api';
 import { event, facts, figures, iso, NOW } from './fixtures';
-import { Overview, REREAD_AFTER_MS } from './model.svelte';
+import { Model, REREAD_AFTER_MS } from './model.svelte';
 
 /** A `Client` answering each path from `answers`; a path not there fails. */
 function api(answers: Record<string, unknown>): Client & { asked: string[] } {
@@ -14,7 +14,7 @@ function api(answers: Record<string, unknown>): Client & { asked: string[] } {
       return answers[path] as T;
     },
     send: async () => {
-      throw new Error('the Overview changes nothing');
+      throw new Error('the model changes nothing');
     },
   };
 }
@@ -43,7 +43,7 @@ const READS = {
   },
 };
 
-describe("the Overview's data", () => {
+describe("the console's model", () => {
   beforeEach(() => {
     vi.useFakeTimers();
     vi.setSystemTime(NOW);
@@ -51,7 +51,7 @@ describe("the Overview's data", () => {
   afterEach(() => vi.useRealTimers());
 
   it('is read part by part; a part that fails keeps what it had and says why', async () => {
-    const o = new Overview(api(READS));
+    const o = new Model(api(READS));
     await o.load();
     expect(o.facts.pipelines.map((p) => p.id)).toEqual(['orders-pg', 'billing-mysql', 'inventory', 'customers']);
     expect(o.facts.actions.customers.kind).toBe('action.pipeline.resume');
@@ -59,7 +59,7 @@ describe("the Overview's data", () => {
     expect(o.facts.lag['orders-pg']).toEqual([[NOW / 1000, 21]]);
     expect(Object.values(o.read).every((r) => r.at === NOW && r.error === null)).toBe(true);
     const { '/v1/snapshots': _, ...without } = READS;
-    const later = new Overview(api(without));
+    const later = new Model(api(without));
     later.facts.jobs = facts().jobs;
     await later.load();
     expect(later.read.jobs).toEqual({ at: null, error: '/v1/snapshots is unavailable' });
@@ -68,7 +68,7 @@ describe("the Overview's data", () => {
   });
 
   it("follows the stream: the newest owner's figures, a state, the lag's last hour", () => {
-    const o = new Overview(api({}));
+    const o = new Model(api({}));
     o.facts = facts();
     o.figures(figures({ id: 'billing-mysql', node: 'n2', owner_epoch: 2, worker: { lag_seconds: 1, degraded: null } }));
     expect(o.facts.figures['billing-mysql'].node).toBe('n1');
@@ -89,7 +89,7 @@ describe("the Overview's data", () => {
 
   it('reads the gap when a node is followed again, and forgets a node gone from the cluster', async () => {
     const client = api(READS);
-    const o = new Overview(client);
+    const o = new Model(client);
     o.facts = facts();
     o.peer({ peer: 'n3', state: 'following' });
     await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
@@ -111,7 +111,7 @@ describe("the Overview's data", () => {
   });
 
   it('keeps only accepted pauses, stops and resumes, read or streamed', () => {
-    const o = new Overview(api({}));
+    const o = new Model(api({}));
     o.facts = facts();
     const before = o.facts.actions.inventory;
     o.event(event({ key: 'refused', kind: 'action.pipeline.resume', at: iso(NOW), pipeline: 'inventory', severity: 'warning', outcome: 'refused', actor: { name: 'ci-bot', role: 'operator' } }));
@@ -122,7 +122,7 @@ describe("the Overview's data", () => {
   });
 
   it("forgets a removed pipeline's figures, lag, actions and lease", async () => {
-    const o = new Overview(api({ '/v1/pipelines': { pipelines: facts().pipelines.filter((p) => p.id !== 'orders-pg') } }));
+    const o = new Model(api({ '/v1/pipelines': { pipelines: facts().pipelines.filter((p) => p.id !== 'orders-pg') } }));
     o.facts = facts();
     o.facts.actions['orders-pg'] = o.facts.actions.inventory;
     o.facts.unheld = { 'orders-pg': NOW, customers: NOW };
@@ -135,7 +135,7 @@ describe("the Overview's data", () => {
 
   it("keeps the hour's lease losses: a streamed one joins them once", async () => {
     const client = api(READS);
-    const o = new Overview(client);
+    const o = new Model(client);
     o.facts = facts();
     const lost = event({ kind: 'lease.lost', at: iso(NOW), pipeline: 'customers', text: 'n1 lost the lease of customers' });
     o.event(lost);
@@ -151,7 +151,7 @@ describe("the Overview's data", () => {
   it('remembers since when a lease has had no holder, until one holds it', async () => {
     const status = structuredClone(facts().cluster!);
     status.pipelines[3].holder = null;
-    const o = new Overview(api({ '/v1/cluster/status': status }));
+    const o = new Model(api({ '/v1/cluster/status': status }));
     await o.load(['cluster']);
     expect(o.facts.unheld).toEqual({ customers: NOW });
     vi.setSystemTime(NOW + 20_000);
@@ -165,7 +165,7 @@ describe("the Overview's data", () => {
 
   it('lists an event once, and reads again what it changed, a burst at once', async () => {
     const client = api(READS);
-    const o = new Overview(client);
+    const o = new Model(client);
     o.facts = facts();
     const paused = event({ kind: 'action.pipeline.pause', at: iso(NOW), pipeline: 'customers', actor: { name: 'ci-bot', role: 'operator' }, outcome: 'accepted' });
     o.event(paused);
````

`ui/src/overview/types.ts` → `ui/src/model/types.ts`:

````diff
diff --git a/ui/src/overview/types.ts b/ui/src/model/types.ts
similarity index 79%
rename from ui/src/overview/types.ts
rename to ui/src/model/types.ts
index a77c4c7fb6fbe89b7fe8b013678ee7a46b5916d8..ca882d143492edc1a883cbdb77fd7817a797fb5a 100644
--- a/ui/src/overview/types.ts
+++ b/ui/src/model/types.ts
@@ -1,9 +1,24 @@
-// What the Overview reads from `/v1` and the live stream: the fields it uses, as the server sends
+// What the console reads from `/v1` and the live stream: the fields it uses, as the server sends
 // them (the API's reference names the rest).
 
 export type Actual = 'starting' | 'running' | 'paused' | 'stopped' | 'dumped' | 'retrying' | 'error';
 export type Desired = 'running' | 'paused' | 'stopped';
 
+/** Where a pipeline reads: its config's `source`. */
+export interface SourceAt {
+  type: 'postgres' | 'mysql';
+  host: string;
+  port: number;
+  db: string;
+}
+
+/** Where a pipeline writes: its config's `sink`. */
+export interface SinkAt {
+  type: 'delta' | 'iceberg';
+  bucket: string;
+  prefix: string;
+}
+
 /** `GET /v1/pipelines`'s rows. */
 export interface PipelineItem {
   id: string;
@@ -12,13 +27,24 @@ export interface PipelineItem {
   actual_state: Actual | null;
   holder: string | null;
   holder_stale: boolean;
+  source: SourceAt;
+  sink: SinkAt;
 }
 
 /** `GET /v1/cluster/status`. */
 export interface ClusterStatus {
   node: string;
   members: { node: string; phase: string; suspect: boolean; config_generation: number | null }[];
-  pipelines: { id: string; holder: string | null; holder_stale: boolean; owner_epoch: number }[];
+  pipelines: {
+    id: string;
+    holder: string | null;
+    holder_stale: boolean;
+    owner_epoch: number;
+    /** When the lease ends unless renewed (RFC 3339). */
+    expires_at: string | null;
+    /** The node the ring gives it to: a holder that isn't it holds it away from its owner. */
+    ring_owner: string | null;
+  }[];
 }
 
 /** `GET /v1/config/nodes`. */
````

`ui/src/overview/Overview.svelte`:

````diff
diff --git a/ui/src/overview/Overview.svelte b/ui/src/overview/Overview.svelte
index fed4b69bcd9fb7a9f66bcb8c8f95d32cac24f7b8..2d67070249d9c0d8b053d27523f0757be2fc4ba9 100644
--- a/ui/src/overview/Overview.svelte
+++ b/ui/src/overview/Overview.svelte
@@ -15,11 +15,11 @@
     nodeHref,
   } from './attention';
   import { rows, tiles } from './derive';
-  import { EVENTS_SHOWN, PARTS, type Overview } from './model.svelte';
+  import { EVENTS_SHOWN, PARTS, type Model } from '../model/model.svelte';
   import Sparkline from './Sparkline.svelte';
 
   interface Props {
-    model: Overview;
+    model: Model;
     now: number;
     /** The live updates stopped: every number is as of `last`. */
     stale: boolean;
````

`ui/src/overview/Overview.test.ts`:

````diff
diff --git a/ui/src/overview/Overview.test.ts b/ui/src/overview/Overview.test.ts
index ff15e271dd5e97564bb36c1ba161896e34b735ed..86f2d5b21bcee519bf1990c02c270d39ef4bbb3a 100644
--- a/ui/src/overview/Overview.test.ts
+++ b/ui/src/overview/Overview.test.ts
@@ -2,8 +2,8 @@ import { flushSync, mount, unmount } from 'svelte';
 import { afterEach, describe, expect, it } from 'vitest';
 import type { Client } from '../lib/api';
 import { clock } from '../lib/format';
-import { facts, NOW } from './fixtures';
-import { Overview as Model } from './model.svelte';
+import { facts, NOW } from '../model/fixtures';
+import { Model } from '../model/model.svelte';
 import Overview from './Overview.svelte';
 
 let mounted: ReturnType<typeof mount> | undefined;
````

`ui/src/overview/attention.test.ts`:

````diff
diff --git a/ui/src/overview/attention.test.ts b/ui/src/overview/attention.test.ts
index aef7b0c3018c9a1734fa7b76c31300b9110476f4..9201149fe1e708a23c910c76d4c977e898ca9c82 100644
--- a/ui/src/overview/attention.test.ts
+++ b/ui/src/overview/attention.test.ts
@@ -1,6 +1,6 @@
 import { describe, expect, it } from 'vitest';
 import { attention, LAG_TARGET_S } from './attention';
-import { event, facts, figures, iso, NOW } from './fixtures';
+import { event, facts, figures, iso, NOW } from '../model/fixtures';
 
 const MIN = 60_000;
 const GIB = 1024 ** 3;
````

`ui/src/overview/attention.ts`:

````diff
diff --git a/ui/src/overview/attention.ts b/ui/src/overview/attention.ts
index 151596289113e294eacdd13389ccaaeb00a67203..33ddb1c107536a5bc4144da3876e05194864f956 100644
--- a/ui/src/overview/attention.ts
+++ b/ui/src/overview/attention.ts
@@ -3,8 +3,8 @@
 // 2026-11-25: a later release makes them configurable).
 
 import { bytes, duration } from '../lib/format';
-import { at, lagOf, tablesOf, type Facts } from './facts';
-import type { EventItem } from './types';
+import { at, lagOf, tablesOf, type Facts } from '../model/facts';
+import type { EventItem } from '../model/types';
 
 /** The lag a pipeline is expected to stay under. */
 export const LAG_TARGET_S = 60;
````

`ui/src/overview/derive.test.ts`:

````diff
diff --git a/ui/src/overview/derive.test.ts b/ui/src/overview/derive.test.ts
index a257957db21eefeb771b7d4300fc64386a46c49b..e959dad09e0a07a620ff344a7011d3c0496ef66a 100644
--- a/ui/src/overview/derive.test.ts
+++ b/ui/src/overview/derive.test.ts
@@ -1,6 +1,6 @@
 import { describe, expect, it } from 'vitest';
 import { rows, tiles } from './derive';
-import { facts, figures, NOW } from './fixtures';
+import { facts, figures, NOW } from '../model/fixtures';
 
 describe("the Overview's", () => {
   it("tiles are mockup 01's", () => {
````

`ui/src/overview/derive.ts`:

````diff
diff --git a/ui/src/overview/derive.ts b/ui/src/overview/derive.ts
index 4a358d17ef183c1a2e7143b3684b85a0ab0f5a33..606588f1fdfe9ed6dbf11614a2e6602241dd2013 100644
--- a/ui/src/overview/derive.ts
+++ b/ui/src/overview/derive.ts
@@ -2,7 +2,7 @@
 
 import { ago, bytes, duration } from '../lib/format';
 import { FAILED_WITHIN_S, LAG_TARGET_S, walPast } from './attention';
-import { at, lagOf, tablesOf, type Facts } from './facts';
+import { at, lagOf, tablesOf, type Facts } from '../model/facts';
 
 export type Tone = 'normal' | 'green' | 'orange' | 'red' | 'grey';
 
````

- [ ] **Step 2: Run them to see them fail**

Run: `(cd ui && npm test); cargo test -p data-avalanche-api pipelines::`
Expected: `facts.test.ts` fails: `route is not a function` (twice); the 58 other UI tests pass;
`the_list_has_every_pipeline_s_summary` fails, the row without `source` and `sink` (the other
pipeline tests pass).

- [ ] **Step 3: Implement**

`summary` writes `source` and `sink`, and `get` no longer adds them; `route` in `facts.ts`. Then
regenerate the API's documents: `DATA_AVALANCHE_BLESS=1 cargo test -p data-avalanche-api openapi`
and `cargo xtask docs`.

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index b64fe2241001bd25562093bbe5ae464521352140..11e538e489289996e4127e7243a2d4f04569e89d 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -372,7 +372,7 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
                 "GET",
                 "/v1/pipelines",
                 Viewer,
-                "Every pipeline: its desired and actual states and its holder",
+                "Every pipeline: its desired and actual states, its holder, and its source and sink",
                 &[(200, "the pipelines")],
             ),
             get(pipelines::list),
````

`crates/data-avalanche-api/src/pipelines.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/pipelines.rs b/crates/data-avalanche-api/src/pipelines.rs
index 6be7f586740f10a90b7f985aaa379c03d1cd9bd3..25867a5469cad8d9f54793451541d4fe5959c85a 100644
--- a/crates/data-avalanche-api/src/pipelines.rs
+++ b/crates/data-avalanche-api/src/pipelines.rs
@@ -129,6 +129,8 @@ pub(crate) fn liveness(members: &Members, lease: &LeaseDoc) -> Liveness {
     }
 }
 
+/// A pipeline as the list shows it: its states, its holder, and where it reads and writes (the
+/// console's list names them, Admin_Console §4).
 fn summary(p: &Pipeline, r: &Read, members: &Members) -> Value {
     let (actual, _) = actual(r.lease.as_ref(), r.status.as_ref());
     let held = r.lease.as_ref().filter(|l| !l.released);
@@ -139,6 +141,13 @@ fn summary(p: &Pipeline, r: &Read, members: &Members) -> Value {
         "actual_state": actual,
         "holder": held.map(|l| l.holder_node_id.clone()),
         "holder_stale": held.map(|l| liveness(members, l) == Liveness::Stale),
+        "source": {
+            "type": p.source.kind.as_str(),
+            "host": p.source.host,
+            "port": p.source.port,
+            "db": p.source.db,
+        },
+        "sink": { "type": p.sink.kind.as_str(), "bucket": p.sink.bucket, "prefix": p.sink.prefix },
     })
 }
 
@@ -213,14 +222,6 @@ pub(crate) async fn get(State(api): State<Api>, Path(id): Path<String>) -> Respo
     let (actual, status) = actual(r.lease.as_ref(), r.status.as_ref());
     let members = node.members.borrow().clone();
     let mut v = summary(&p, &r, &members);
-    v["source"] = json!({
-        "type": p.source.kind.as_str(),
-        "host": p.source.host,
-        "port": p.source.port,
-        "db": p.source.db,
-    });
-    v["sink"] =
-        json!({ "type": p.sink.kind.as_str(), "bucket": p.sink.bucket, "prefix": p.sink.prefix });
     v["actual_state"] = json!(actual);
     v["error"] = json!(status.and_then(|s| s.error.clone()));
     v["code"] = json!(status.and_then(|s| s.code.clone()));
````

`ui/src/model/facts.ts`:

````diff
diff --git a/ui/src/model/facts.ts b/ui/src/model/facts.ts
index 312c2e780a59828f913cc915cc941678770ca72f..06592333f114f11b0b531b43b4615d8eac7568f2 100644
--- a/ui/src/model/facts.ts
+++ b/ui/src/model/facts.ts
@@ -8,6 +8,8 @@ import type {
   Figures,
   Job,
   PipelineItem,
+  SinkAt,
+  SourceAt,
   TableItem,
 } from './types';
 
@@ -74,3 +76,14 @@ export function at(t: string | null | undefined): number | null {
   const ms = Date.parse(t);
   return Number.isNaN(ms) ? null : ms;
 }
+
+const SHORT = { postgres: 'PG', mysql: 'MY', delta: 'Δ', iceberg: 'Iceberg' } as const;
+const LONG = { postgres: 'PostgreSQL', mysql: 'MySQL', delta: 'Delta', iceberg: 'Iceberg' } as const;
+
+/** Where a pipeline reads and writes: `PG orders@pg-prod → Δ lake/orders`, the engines' names
+ * written out with `long`. */
+export function route(p: { source: SourceAt; sink: SinkAt }, long = false): string {
+  const n = long ? LONG : SHORT;
+  const lake = [p.sink.bucket, p.sink.prefix.replace(/^\/+|\/+$/g, '')].filter((s) => s !== '').join('/');
+  return `${n[p.source.type]} ${p.source.db}@${p.source.host} → ${n[p.sink.type]} ${lake}`;
+}
````

Regenerated, not written by hand: `crates/data-avalanche-api/openapi.json`, `docs/public/openapi.json`, `docs/src/content/docs/reference/api.md`.

- [ ] **Step 4: Run the tests and the gates**

Run the step's tests again, then

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
git add -A crates/data-avalanche-api/openapi.json crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/pipelines.rs docs/public/openapi.json docs/src/content/docs/reference/api.md ui/src/App.svelte ui/src/model/facts.test.ts ui/src/model/facts.ts ui/src/model/fixtures.ts ui/src/model/model.svelte.ts ui/src/model/model.test.ts ui/src/model/types.ts ui/src/overview/Overview.svelte ui/src/overview/Overview.test.ts ui/src/overview/attention.test.ts ui/src/overview/attention.ts ui/src/overview/derive.test.ts ui/src/overview/derive.ts ui/src/overview/facts.ts ui/src/overview/fixtures.ts ui/src/overview/model.svelte.ts ui/src/overview/model.test.ts ui/src/overview/types.ts
git commit -m "feat(ui): the console's shared model; /v1/pipelines names each pipeline's source and sink"
```

---

### Task 2: The chart (`ui/src/lib/series.ts`, `Chart.svelte`, uPlot)

**Files:**
- Regenerate: `ui/package-lock.json`
- Modify: `ui/package.json`
- Create: `ui/src/lib/Chart.svelte`
- Create: `ui/src/lib/Chart.test.ts`
- Create: `ui/src/lib/series.test.ts`
- Create: `ui/src/lib/series.ts`
- Modify: `xtask/src/deps.rs`

**Interfaces:**
- Consumes: `format.ts`'s `bytes`, `clock`, `duration`.
- Produces (`ui/src/lib/series.ts`, D-M16a.2): `type Unit = 'seconds' | 'bytes' | 'rows/s' |
  'changes/min' | 'files' | 'cores'`; `interface Mark { key: string; at: number; label: string }`
  (unix seconds); `interface Placed extends Mark { x: number; row: number }`; `LABEL_ROWS` (3),
  `CHAR_PX` (6.5), `LABEL_PAD_PX` (10), `HOVER_PX` (4); `placeLabels(marks, x, since, until):
  Placed[]`; `markNear(placed, px): string | null`; `compact(n): string` (`41 k`, `2.6 M`);
  `formatValue(unit, v): string`; `readout(at, v, unit): string`; `type Pen`, `interface Area`,
  `interface Ink`; `drawOverlay(pen, area, targetY, marks, hovered, ink, ratio): void`.
- Produces (`ui/src/lib/Chart.svelte`): props `label: string`, `unit: Unit`, `points: [number,
  number][]`, `since: number`, `until: number`, `target?: number | null`, `marks?: Mark[]`,
  `hovered?: string | null`, `onHover?: (key: string | null) => void`, `height?: number` (180).
- Produces (`xtask/src/deps.rs`): `npm()` reads `ui/package.json`'s `dependencies` too.

- [ ] **Step 1: Write the failing tests**

`npm install --save-exact uplot@1.6.32` in `ui/` adds the package (and writes the lock). The
tests: `a chart's event labels` (stacked when they'd overlap, kept to the range; the last line once
every line is taken; hovered within 4 px), `a chart's values` (each unit), `a chart's overlay`
(`series.test.ts`); `a chart` (`Chart.test.ts`, uPlot replaced by a fake that records what it was
given and runs its hooks: the series, the value now and the labels; no samples; a label hovered,
and one hovered in the page; the pointer's readout and the event under it; uPlot destroyed with
it); `the_inventory_covers_every_kind` asserts `has(Kind::Npm, "uplot")`.

`ui/package.json`:

````diff
diff --git a/ui/package.json b/ui/package.json
index 51a2f7799b5e008f41d63041737f1d3d41812e4d..a7f4e627213a4d79c77a0780be112d67bd778b8d 100644
--- a/ui/package.json
+++ b/ui/package.json
@@ -18,5 +18,8 @@
     "typescript": "6.0.3",
     "vite": "8.3.8",
     "vitest": "5.1.1"
+  },
+  "dependencies": {
+    "uplot": "1.6.32"
   }
 }
````

`ui/src/lib/Chart.test.ts`:

````diff
diff --git a/ui/src/lib/Chart.test.ts b/ui/src/lib/Chart.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..859c3224f3cc8e0ee8e909e05e909eaa72561c55
--- /dev/null
+++ b/ui/src/lib/Chart.test.ts
@@ -0,0 +1,118 @@
+import { flushSync, mount, tick, unmount } from 'svelte';
+import { afterEach, describe, expect, it, vi } from 'vitest';
+import Chart from './Chart.svelte';
+
+/** uPlot as far as the chart uses it: a second a pixel from the range's start. */
+interface Fake {
+  data: number[][];
+  destroyed: boolean;
+  hover(idx: number, left: number): void;
+}
+const made: Fake[] = [];
+vi.mock('uplot', () => ({
+  default: class {
+    bbox = { left: 120, top: 0, width: 1200, height: 300 };
+    ctx = { save() {}, restore() {}, beginPath() {}, moveTo() {}, lineTo() {}, stroke() {}, setLineDash() {}, lineWidth: 0, strokeStyle: '' };
+    cursor: { idx: number | null; left: number } = { idx: null, left: -10 };
+    data: number[][];
+    destroyed = false;
+    #opts: { scales: { x: { min: number } }; hooks: { draw: ((u: unknown) => void)[]; setCursor: ((u: unknown) => void)[] } };
+    constructor(opts: never, data: number[][]) {
+      this.data = data;
+      this.#opts = opts;
+      made.push(this);
+      this.redraw();
+    }
+    valToPos(v: number, axis: string) {
+      return axis === 'x' ? v - this.#opts.scales.x.min : 0;
+    }
+    setData(data: number[][]) {
+      this.data = data;
+    }
+    setScale() {}
+    setSize() {}
+    redraw() {
+      for (const h of this.#opts.hooks.draw) h(this);
+    }
+    hover(idx: number, left: number) {
+      this.cursor = { idx, left };
+      for (const h of this.#opts.hooks.setCursor) h(this);
+    }
+    destroy() {
+      this.destroyed = true;
+    }
+  },
+}));
+
+let mounted: ReturnType<typeof mount> | undefined;
+afterEach(() => {
+  if (mounted !== undefined) unmount(mounted);
+  mounted = undefined;
+  made.length = 0;
+});
+
+const points: [number, number][] = [
+  [1000, 2],
+  [1060, 4],
+  [1120, 94],
+];
+const marks = [
+  { key: 'e1', at: 1030, label: 'restarted' },
+  { key: 'e2', at: 1100, label: 'failover' },
+];
+
+async function render(props: Record<string, unknown>): Promise<HTMLElement> {
+  const target = document.createElement('div');
+  document.body.replaceChildren(target);
+  mounted = mount(Chart, { target, props: { label: 'Lag', unit: 'seconds', points, since: 1000, until: 1200, ...props } as never });
+  // uPlot is loaded on its own; the chart is drawn once it is.
+  await vi.dynamicImportSettled();
+  await tick();
+  flushSync();
+  return target;
+}
+
+describe('a chart', () => {
+  it('draws the series over its range, its value now, and its events as labels', async () => {
+    const t = await render({ target: 60, marks });
+    expect(made[0].data).toEqual([
+      [1000, 1060, 1120],
+      [2, 4, 94],
+    ]);
+    expect(t.querySelector('.at')?.textContent).toBe('now 94 s');
+    expect([...t.querySelectorAll('.label')].map((l) => l.textContent)).toEqual(['restarted', 'failover']);
+  });
+
+  it('says so when the range has no samples yet', async () => {
+    const t = await render({ points: [] });
+    expect(t.textContent).toContain('No samples in this range yet.');
+  });
+
+  it("hovers an event from its label, and shows one hovered in the page's list", async () => {
+    const hovered: (string | null)[] = [];
+    const t = await render({ marks, hovered: 'e2', onHover: (k: string | null) => hovered.push(k) });
+    const labels = t.querySelectorAll('.label');
+    expect(labels[1].classList.contains('on')).toBe(true);
+    labels[0].dispatchEvent(new MouseEvent('mouseenter'));
+    labels[0].dispatchEvent(new MouseEvent('mouseleave'));
+    expect(hovered).toEqual(['e1', null]);
+  });
+
+  it('reads the time and value at the pointer, and hovers the event whose line is under it', async () => {
+    const hovered: (string | null)[] = [];
+    const t = await render({ marks, onHover: (k: string | null) => hovered.push(k) });
+    // Over the second sample, 2 px from the first event's line (at 30 px).
+    made[0].hover(1, 32);
+    flushSync();
+    expect(t.querySelector('.at')?.textContent).toMatch(/^\d\d:\d\d:\d\d · 4 s$/);
+    made[0].hover(2, 80);
+    expect(hovered).toEqual(['e1', null]);
+  });
+
+  it('lets uPlot go when it goes', async () => {
+    await render({});
+    unmount(mounted!);
+    mounted = undefined;
+    expect(made[0].destroyed).toBe(true);
+  });
+});
````

`ui/src/lib/series.test.ts`:

````diff
diff --git a/ui/src/lib/series.test.ts b/ui/src/lib/series.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..b0549b0ae5cb1cbf950e6aa002d102c7a774336e
--- /dev/null
+++ b/ui/src/lib/series.test.ts
@@ -0,0 +1,113 @@
+import { describe, expect, it } from 'vitest';
+import {
+  CHAR_PX,
+  compact,
+  drawOverlay,
+  formatValue,
+  LABEL_PAD_PX,
+  LABEL_ROWS,
+  markNear,
+  placeLabels,
+  type Pen,
+} from './series';
+
+const x = (t: number) => t; // a second a pixel
+
+describe("a chart's event labels", () => {
+  it('stack on the next line when they would overlap, and keep to the range', () => {
+    const wide = 'failover'.length * CHAR_PX + LABEL_PAD_PX;
+    const placed = placeLabels(
+      [
+        { key: 'c', at: 120, label: 'snapshot' },
+        { key: 'a', at: 100, label: 'failover' },
+        { key: 'd', at: 100 + wide, label: 'restarted' },
+        { key: 'early', at: 10, label: 'before the range' },
+      ],
+      x,
+      50,
+      1000,
+    );
+    expect(placed.map((p) => [p.key, p.x, p.row])).toEqual([
+      ['a', 100, 0],
+      ['c', 120, 1],
+      ['d', 100 + wide, 0],
+    ]);
+  });
+
+  it(`go on the last of ${LABEL_ROWS} lines once every line is taken`, () => {
+    const marks = Array.from({ length: LABEL_ROWS + 2 }, (_, i) => ({ key: `m${i}`, at: 100 + i, label: 'halted' }));
+    expect(placeLabels(marks, x, 0, 1000).map((p) => p.row)).toEqual([0, 1, 2, 2, 2]);
+  });
+
+  it('are hovered when the pointer is within 4 px of their line, the nearest first', () => {
+    const placed = placeLabels(
+      [
+        { key: 'a', at: 100, label: 'a' },
+        { key: 'b', at: 106, label: 'b' },
+      ],
+      x,
+      0,
+      1000,
+    );
+    expect(markNear(placed, 103)).toBe('a');
+    expect(markNear(placed, 104)).toBe('b');
+    expect(markNear(placed, 111)).toBeNull();
+  });
+});
+
+describe("a chart's values", () => {
+  it('are written in their units, short', () => {
+    expect(formatValue('seconds', 94)).toBe('94 s');
+    expect(formatValue('seconds', 252)).toBe('4m 12s');
+    expect(formatValue('bytes', 210 * 1024 ** 2)).toBe('210 MB');
+    expect(formatValue('rows/s', 41_200)).toBe('41 k/s');
+    expect(formatValue('changes/min', 2_600_000)).toBe('2.6 M/min');
+    expect(formatValue('files', 14)).toBe('14');
+    expect(formatValue('cores', 0.62)).toBe('0.6');
+    expect(compact(1_000_000)).toBe('1 M');
+  });
+});
+
+describe("a chart's overlay", () => {
+  function pen(): Pen & { lines: string[] } {
+    const lines: string[] = [];
+    let from = '';
+    let dash: number[] = [];
+    const p = {
+      lines,
+      lineWidth: 0,
+      strokeStyle: '',
+      save() {},
+      restore() {},
+      beginPath() {},
+      setLineDash(d: number[]) {
+        dash = d;
+      },
+      moveTo(x: number, y: number) {
+        from = `${x},${y}`;
+      },
+      lineTo(x: number, y: number) {
+        lines.push(`${from}→${x},${y} ${p.strokeStyle} w${p.lineWidth} [${dash.join(' ')}]`);
+      },
+      stroke() {},
+    };
+    return p;
+  }
+
+  it('draws the target dashed across, and each event dotted down, the hovered one wider', () => {
+    const p = pen();
+    const ink = { target: 'grey', mark: 'dim', hovered: 'blue' };
+    drawOverlay(p, { left: 10, top: 5, width: 100, height: 50 }, 30, [{ key: 'a', x: 40 }, { key: 'b', x: 70 }], 'b', ink, 2);
+    expect(p.lines).toEqual([
+      '10,30→110,30 grey w2 [12 8]',
+      '40,5→40,55 dim w2 [4 6]',
+      '70,5→70,55 blue w4 [4 6]',
+    ]);
+  });
+
+  it('draws no target without one', () => {
+    const p = pen();
+    drawOverlay(p, { left: 0, top: 0, width: 10, height: 10 }, null, [], null, { target: 't', mark: 'm', hovered: 'h' }, 1);
+    expect(p.lines).toEqual([]);
+  });
+});
````

`xtask/src/deps.rs`:

````diff
diff --git a/xtask/src/deps.rs b/xtask/src/deps.rs
index 53878374f60870def9fbcf410908ffe857cfec0c..155ac812554150956e9caf642312bcbbb97fa37a 100644
--- a/xtask/src/deps.rs
+++ b/xtask/src/deps.rs
@@ -1077,6 +1077,7 @@ mod tests {
         assert!(has(Kind::Tool, "duckdb"));
         assert!(has(Kind::Npm, "astro"));
         assert!(has(Kind::Npm, "svelte"));
+        assert!(has(Kind::Npm, "uplot"));
         assert!(has(Kind::Node, "node"));
         assert!(
             items
````

Regenerated, not written by hand: `ui/package-lock.json`.

- [ ] **Step 2: Run them to see them fail**

Run: `(cd ui && npm test); cargo test -p xtask deps::`
Expected: `Chart.test.ts` and `series.test.ts` fail to load (`Failed to resolve import
"./Chart.svelte"`, `"./series"`); `the_inventory_covers_every_kind` fails at
`assert!(has(Kind::Npm, "uplot"))` (the others pass).

- [ ] **Step 3: Implement**

`series.ts`, `Chart.svelte`, and `npm()` reading both sections of the console's `package.json`.

`ui/src/lib/Chart.svelte`:

````diff
diff --git a/ui/src/lib/Chart.svelte b/ui/src/lib/Chart.svelte
new file mode 100644
index 0000000000000000000000000000000000000000..7ae877f4b96e01c0e7371b1e638be5d71cf97ec8
--- /dev/null
+++ b/ui/src/lib/Chart.svelte
@@ -0,0 +1,204 @@
+<script lang="ts">
+  // A time series as Admin_Console §4.2 draws every one: axes with units and times, the target as
+  // a dashed line, events as dotted lines with short labels stacked when close, a crosshair with
+  // the time and value at the pointer. Hovering a line or a label hovers its event, which the page
+  // highlights in its list too, and the reverse. uPlot is loaded with the first chart, so a page
+  // without one never loads it.
+  import { onMount } from 'svelte';
+  import type uPlot from 'uplot';
+  import 'uplot/dist/uPlot.min.css';
+  import {
+    drawOverlay,
+    formatValue,
+    markNear,
+    placeLabels,
+    readout,
+    type Mark,
+    type Placed,
+    type Unit,
+  } from './series';
+
+  interface Props {
+    label: string;
+    unit: Unit;
+    /** `[unix seconds, value]`, oldest first. */
+    points: [number, number][];
+    since: number;
+    until: number;
+    target?: number | null;
+    marks?: Mark[];
+    /** The event hovered, here or in the page's list. */
+    hovered?: string | null;
+    onHover?: (key: string | null) => void;
+    height?: number;
+  }
+  let {
+    label,
+    unit,
+    points,
+    since,
+    until,
+    target = null,
+    marks = [],
+    hovered = null,
+    onHover = () => {},
+    height = 180,
+  }: Props = $props();
+
+  let box: HTMLDivElement;
+  let plot: uPlot | null = null;
+  let placed = $state<Placed[]>([]);
+  /** Where the plot's area starts in the box (CSS pixels): the labels sit above it. */
+  let left = $state(0);
+  let at = $state<string | null>(null);
+
+  const data = $derived([points.map(([t]) => t), points.map(([, v]) => v)] as uPlot.AlignedData);
+  const now = $derived(points.length > 0 ? formatValue(unit, points[points.length - 1][1]) : null);
+
+  function ink() {
+    const css = getComputedStyle(box);
+    return {
+      target: css.getPropertyValue('--grey').trim() || 'grey',
+      mark: css.getPropertyValue('--text-2').trim() || 'grey',
+      hovered: css.getPropertyValue('--accent').trim() || 'blue',
+    };
+  }
+
+  function draw(u: uPlot) {
+    const ratio = devicePixelRatio || 1;
+    const p = placeLabels(marks, (t) => u.valToPos(t, 'x'), since, until);
+    placed = p;
+    left = u.bbox.left / ratio;
+    drawOverlay(
+      u.ctx,
+      u.bbox,
+      target === null ? null : u.valToPos(target, 'y', true),
+      p.map((m) => ({ key: m.key, x: u.valToPos(m.at, 'x', true) })),
+      hovered,
+      ink(),
+      ratio,
+    );
+  }
+
+  function cursor(u: uPlot) {
+    const idx = u.cursor.idx;
+    const x = u.cursor.left;
+    if (idx === null || idx === undefined || x === undefined || x < 0) {
+      at = null;
+      return;
+    }
+    at = readout(u.data[0][idx], u.data[1][idx] ?? null, unit);
+    onHover(markNear(placed, x));
+  }
+
+  onMount(() => {
+    let gone = false;
+    let resize: ResizeObserver | undefined;
+    void import('uplot').then(({ default: UPlot }) => {
+      if (gone) return;
+      plot = new UPlot(
+        {
+          width: box.clientWidth || 600,
+          height,
+          legend: { show: false },
+          scales: { x: { time: true, min: since, max: until }, y: { range: (_u, _min, max) => [0, Math.max(max, target ?? 0) * 1.1 || 1] } },
+          axes: [{}, { values: (_u, ticks) => ticks.map((v) => formatValue(unit, v)), size: 60 }],
+          series: [{}, { label, stroke: ink().hovered, width: 1.5, points: { show: false } }],
+          hooks: { draw: [draw], setCursor: [cursor] },
+        },
+        data,
+        box,
+      );
+      if (typeof ResizeObserver !== 'undefined') {
+        resize = new ResizeObserver(() => plot?.setSize({ width: box.clientWidth, height }));
+        resize.observe(box);
+      }
+    });
+    return () => {
+      gone = true;
+      resize?.disconnect();
+      plot?.destroy();
+      plot = null;
+    };
+  });
+
+  // New samples, a new range, or another event hovered: drawn again.
+  $effect(() => {
+    plot?.setData(data, false);
+    plot?.setScale('x', { min: since, max: until });
+  });
+  $effect(() => {
+    void hovered;
+    void marks;
+    plot?.redraw(false);
+  });
+</script>
+
+<figure class="chart">
+  <figcaption>
+    <span>{label}</span>
+    {#if at !== null}<span class="at">{at}</span>{:else if now !== null}<span class="at">now {now}</span>{/if}
+  </figcaption>
+  <div class="labels" style:left="{left}px">
+    {#each placed as m (m.key)}
+      <button
+        type="button"
+        class="label"
+        class:on={m.key === hovered}
+        style:left="{m.x}px"
+        style:top="{m.row * 16}px"
+        onmouseenter={() => onHover(m.key)}
+        onmouseleave={() => onHover(null)}
+        onfocus={() => onHover(m.key)}
+        onblur={() => onHover(null)}>{m.label}</button
+      >
+    {/each}
+  </div>
+  <div class="plot" bind:this={box}></div>
+  {#if points.length === 0}
+    <p class="empty muted">No samples in this range yet.</p>
+  {/if}
+</figure>
+
+<style>
+  .chart {
+    position: relative;
+    margin: 0;
+  }
+  figcaption {
+    display: flex;
+    justify-content: space-between;
+    color: var(--text-2);
+    margin-bottom: 4px;
+  }
+  .at {
+    font-variant-numeric: tabular-nums;
+    color: var(--text);
+  }
+  .labels {
+    position: relative;
+    height: 48px;
+  }
+  .label {
+    position: absolute;
+    padding: 0 4px;
+    border: none;
+    background: none;
+    color: var(--text-2);
+    font-size: 11px;
+    white-space: nowrap;
+  }
+  .label.on {
+    color: var(--accent);
+    font-weight: 600;
+  }
+  .empty {
+    position: absolute;
+    top: 50%;
+    width: 100%;
+    text-align: center;
+  }
+  .plot :global(.u-axis) {
+    color: var(--text-2);
+  }
+</style>
````

`ui/src/lib/series.ts`:

````diff
diff --git a/ui/src/lib/series.ts b/ui/src/lib/series.ts
new file mode 100644
index 0000000000000000000000000000000000000000..714e7813f5468f2886bde29f42624832336c05bd
--- /dev/null
+++ b/ui/src/lib/series.ts
@@ -0,0 +1,146 @@
+// The console's time series (Admin_Console §4.2): what a chart draws beside its line, worked out
+// apart from the drawing so it can be tested. Times are unix seconds, as `/v1/history` gives them.
+
+import { bytes, clock, duration } from './format';
+
+/** The units `/v1/history`'s series are in. */
+export type Unit = 'seconds' | 'bytes' | 'rows/s' | 'changes/min' | 'files' | 'cores';
+
+/** An event drawn on a chart: a thin dotted line at its time, with a short label. */
+export interface Mark {
+  /** The event's key: hovering it here highlights it in the page's list, and the reverse. */
+  key: string;
+  at: number;
+  label: string;
+}
+
+/** A mark placed on the chart: `x` in the plot's CSS pixels, `row` its label's line from the top. */
+export interface Placed extends Mark {
+  x: number;
+  row: number;
+}
+
+/** The lines labels stack on before they overlap. */
+export const LABEL_ROWS = 3;
+/** A label's width: its characters at the page's small text, and its padding. */
+export const CHAR_PX = 6.5;
+export const LABEL_PAD_PX = 10;
+/** How near the pointer must be to a mark's line to hover it. */
+export const HOVER_PX = 4;
+
+/** The marks between `since` and `until`, oldest first, each label on the first line where it
+ * doesn't overlap the label before it; past `LABEL_ROWS` lines, on the last. */
+export function placeLabels(
+  marks: Mark[],
+  x: (at: number) => number,
+  since: number,
+  until: number,
+): Placed[] {
+  const ends: number[] = Array.from({ length: LABEL_ROWS }, () => -Infinity);
+  return marks
+    .filter((m) => m.at >= since && m.at <= until)
+    .sort((a, b) => a.at - b.at)
+    .map((m) => {
+      const px = x(m.at);
+      const free = ends.findIndex((end) => end <= px);
+      const row = free === -1 ? LABEL_ROWS - 1 : free;
+      ends[row] = px + m.label.length * CHAR_PX + LABEL_PAD_PX;
+      return { ...m, x: px, row };
+    });
+}
+
+/** The mark whose line is within `HOVER_PX` of `px`, the nearest; `null` for none. */
+export function markNear(placed: Placed[], px: number): string | null {
+  let best: Placed | null = null;
+  for (const p of placed) {
+    const d = Math.abs(p.x - px);
+    if (d <= HOVER_PX && (best === null || d < Math.abs(best.x - px))) best = p;
+  }
+  return best?.key ?? null;
+}
+
+/** A number in a few characters: `860`, `41 k`, `2.6 M`. */
+export function compact(n: number): string {
+  const a = Math.abs(n);
+  if (a < 1000) return String(Math.round(n));
+  if (a < 1e6) return `${Math.round(n / 1000)} k`;
+  return `${(n / 1e6).toFixed(1).replace(/\.0$/, '')} M`;
+}
+
+/** A value in its unit, as an axis and the readout write it. */
+export function formatValue(unit: Unit, v: number): string {
+  switch (unit) {
+    case 'seconds':
+      return v < 120 ? `${Math.round(v)} s` : duration(v);
+    case 'bytes':
+      return bytes(v);
+    case 'rows/s':
+      return `${compact(v)}/s`;
+    case 'changes/min':
+      return `${compact(v)}/min`;
+    case 'files':
+      return compact(v);
+    case 'cores':
+      return v.toFixed(1);
+  }
+}
+
+/** What the crosshair reads at a point: `13:41:05 · 4 s`. */
+export function readout(at: number, v: number | null, unit: Unit): string {
+  return `${clock(at * 1000, true)} · ${v === null ? 'no sample' : formatValue(unit, v)}`;
+}
+
+/** The part of a drawing context the overlay draws with. */
+export type Pen = Pick<
+  CanvasRenderingContext2D,
+  'save' | 'restore' | 'beginPath' | 'moveTo' | 'lineTo' | 'stroke' | 'setLineDash' | 'lineWidth' | 'strokeStyle'
+>;
+
+/** The plot's area, in canvas pixels. */
+export interface Area {
+  left: number;
+  top: number;
+  width: number;
+  height: number;
+}
+
+/** The colours the overlay draws in, read from the page's theme. */
+export interface Ink {
+  target: string;
+  mark: string;
+  hovered: string;
+}
+
+/** Draws the target as a dashed line across the area at `targetY`, and each mark as a dotted
+ * line down it at its `x` (canvas pixels); the hovered one wider, in its own colour. */
+export function drawOverlay(
+  pen: Pen,
+  area: Area,
+  targetY: number | null,
+  marks: { key: string; x: number }[],
+  hovered: string | null,
+  ink: Ink,
+  ratio: number,
+): void {
+  pen.save();
+  if (targetY !== null) {
+    pen.setLineDash([6 * ratio, 4 * ratio]);
+    pen.lineWidth = ratio;
+    pen.strokeStyle = ink.target;
+    pen.beginPath();
+    pen.moveTo(area.left, targetY);
+    pen.lineTo(area.left + area.width, targetY);
+    pen.stroke();
+  }
+  for (const m of marks) {
+    const on = m.key === hovered;
+    pen.setLineDash([2 * ratio, 3 * ratio]);
+    pen.lineWidth = (on ? 2 : 1) * ratio;
+    pen.strokeStyle = on ? ink.hovered : ink.mark;
+    pen.beginPath();
+    pen.moveTo(m.x, area.top);
+    pen.lineTo(m.x, area.top + area.height);
+    pen.stroke();
+  }
+  pen.restore();
+}
````

`xtask/src/deps.rs`:

````diff
diff --git a/xtask/src/deps.rs b/xtask/src/deps.rs
index 155ac812554150956e9caf642312bcbbb97fa37a..75a0e49e59ed2cc3080580e99a1da9bce423d211 100644
--- a/xtask/src/deps.rs
+++ b/xtask/src/deps.rs
@@ -602,14 +602,22 @@ fn duckdb(root: &Path, items: &mut Vec<Item>) -> Result<(), String> {
     Ok(())
 }
 
-/// The site's packages (`docs/`) and the console's (`ui/`), each with its Node version.
+/// The site's packages (`docs/`) and the console's (`ui/`: what the page runs, uPlot, and what
+/// builds it), each with its Node version.
 fn npm(root: &Path, items: &mut Vec<Item>) -> Result<(), String> {
-    for (dir, section) in [("docs", "dependencies"), ("ui", "devDependencies")] {
+    let sections: [(&str, &[&str]); 2] = [
+        ("docs", &["dependencies"]),
+        ("ui", &["dependencies", "devDependencies"]),
+    ];
+    for (dir, sections) in sections {
         let file = format!("{dir}/package.json");
         let text = read(root, &file)?;
         let json: serde_json::Value =
             serde_json::from_str(&text).map_err(|e| format!("{file}: {e}"))?;
-        for (name, version) in json[section].as_object().into_iter().flatten() {
+        let packages = sections
+            .iter()
+            .flat_map(|s| json[*s].as_object().into_iter().flatten());
+        for (name, version) in packages {
             items.push(Item {
                 kind: Kind::Npm,
                 name: name.clone(),
````

- [ ] **Step 4: Run the tests and the gates**

Run the step's tests again, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
(cd ui && npm run check && npm test && npm run build)
```

`npm run build` writes uPlot to a file of its own only once a page imports `Chart.svelte` (Task 5).

Expected: no output from the first three; every test passes; svelte-check reports 0 errors and
0 warnings.

- [ ] **Step 5: Commit**

```bash
git add -A ui/package-lock.json ui/package.json ui/src/lib/Chart.svelte ui/src/lib/Chart.test.ts ui/src/lib/series.test.ts ui/src/lib/series.ts xtask/src/deps.rs
git commit -m "feat(ui): the console's chart — uPlot, the target, events as labelled lines, hover and crosshair"
```

---

### Task 3: The pipeline actions and their dialog (`ui/src/lib/actions.ts`, `Dialog.svelte`)

**Files:**
- Create: `ui/src/lib/Dialog.svelte`
- Create: `ui/src/lib/Dialog.test.ts`
- Create: `ui/src/lib/actions.test.ts`
- Create: `ui/src/lib/actions.ts`

**Interfaces:**
- Consumes: M15's `Client` (`api.ts`), `Role` and `Session` (`session.ts`); `Actual`, `Desired`
  (`model/types.ts`).
- Produces (`ui/src/lib/actions.ts`, D-M16a.3, D-M16a.5): `type Verb = 'flush' | 'pause' |
  'resume' | 'restart' | 'stop'`; `VERBS`; `NOTE_MAX` (500); `interface Target { id; node: string |
  null; desired: Desired; actual: Actual | null; job: string | null }`; `ACTIONS: Record<Verb, {
  label; past; each; destructive; takes(desired): boolean; effect(t: Target): string }>`;
  `mayAct(role): boolean`; `recordedAs(verb, s: Pick<Session, 'name' | 'role'>): string`;
  `noteBody(note): { note } | undefined`; `ask(api, verb, id, note): Promise<void>`; `interface
  Outcome { done: string[]; failed: { id; error } | null; left: string[] }`; `oneAtATime(ids, each):
  Promise<Outcome>`; `outcomeText(o): string | null`.
- Produces (`ui/src/lib/Dialog.svelte`, D-M16a.4): props `title`, `lines: string[]`, `recorded`,
  `confirm`, `destructive?`, `blocked?: string | null`, `onConfirm(note): Promise<string | null>`
  (`null` when done, else why it failed), `onClose()`, `children?` (the action's own fields).

- [ ] **Step 1: Write the failing tests**

The tests: `pipeline actions` (offered to operators and admins; in the states the server takes
them in; what each does, on which node, the running snapshot a restart carries on; the line it is
recorded as; the note's body) and `an action on several pipelines` (one at a time, stopping at the
first failure; nothing more when every one was asked) in `actions.test.ts`; `an action's dialog`
(`Dialog.test.ts`: what and how it's recorded, the note focused and limited to 500; carried out
with the note, then closed; open with the reason when it fails; blocked; Escape and Cancel; the
destructive style).

`ui/src/lib/Dialog.test.ts`:

````diff
diff --git a/ui/src/lib/Dialog.test.ts b/ui/src/lib/Dialog.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..3165edf2ca6f3b88e9ee96475b5385bf321a369c
--- /dev/null
+++ b/ui/src/lib/Dialog.test.ts
@@ -0,0 +1,90 @@
+import { flushSync, mount, tick, unmount } from 'svelte';
+import { afterEach, describe, expect, it } from 'vitest';
+import Dialog from './Dialog.svelte';
+
+let mounted: ReturnType<typeof mount> | undefined;
+afterEach(() => {
+  if (mounted !== undefined) unmount(mounted);
+  mounted = undefined;
+});
+
+function render(props: Record<string, unknown>): HTMLElement {
+  const target = document.createElement('div');
+  document.body.replaceChildren(target);
+  mounted = mount(Dialog, {
+    target,
+    props: {
+      title: 'Restart orders-pg?',
+      lines: ['orders-pg stops on node-b…'],
+      recorded: 'restarted by Oleksii (operator)',
+      confirm: 'Restart',
+      onConfirm: async () => null,
+      onClose: () => {},
+      ...props,
+    } as never,
+  });
+  flushSync();
+  return target;
+}
+
+const settle = async () => {
+  await tick();
+  await Promise.resolve();
+  flushSync();
+};
+
+function type(t: HTMLElement, note: string) {
+  const area = t.querySelector('textarea')!;
+  area.value = note;
+  area.dispatchEvent(new Event('input'));
+  flushSync();
+}
+
+describe("an action's dialog", () => {
+  it('says what the action does and how it will be recorded, the note field first', () => {
+    const t = render({});
+    expect(t.querySelector('[role="dialog"] h3')?.textContent).toBe('Restart orders-pg?');
+    expect(t.textContent).toContain('orders-pg stops on node-b…');
+    expect(t.textContent).toContain('Recorded as: restarted by Oleksii (operator)');
+    expect(document.activeElement).toBe(t.querySelector('textarea'));
+    expect(t.querySelector('textarea')?.maxLength).toBe(500);
+  });
+
+  it('carries the action out with the note, then closes', async () => {
+    const notes: string[] = [];
+    let closed = 0;
+    const t = render({
+      onConfirm: async (n: string) => (notes.push(n), null),
+      onClose: () => closed++,
+    });
+    type(t, 'apply sink.prefix');
+    t.querySelector<HTMLButtonElement>('.go')!.click();
+    await settle();
+    expect(notes).toEqual(['apply sink.prefix']);
+    expect(closed).toBe(1);
+  });
+
+  it('stays open, saying why, when the action fails', async () => {
+    let closed = 0;
+    const t = render({ onConfirm: async () => 'no node holds its lease', onClose: () => closed++ });
+    t.querySelector<HTMLButtonElement>('.go')!.click();
+    await settle();
+    expect(t.querySelector('[role="alert"]')?.textContent).toBe('no node holds its lease');
+    expect(closed).toBe(0);
+  });
+
+  it("can't be confirmed while blocked, and says why", () => {
+    const t = render({ blocked: 'Offline: actions wait until the console is live again.' });
+    expect(t.querySelector<HTMLButtonElement>('.go')!.disabled).toBe(true);
+    expect(t.textContent).toContain('Offline: actions wait until the console is live again.');
+  });
+
+  it('closes on Escape or Cancel, and marks a destructive action', () => {
+    let closed = 0;
+    const t = render({ destructive: true, onClose: () => closed++ });
+    expect(t.querySelector('.go')?.classList.contains('destructive')).toBe(true);
+    window.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }));
+    t.querySelector<HTMLButtonElement>('.buttons button')!.click();
+    expect(closed).toBe(2);
+  });
+});
````

`ui/src/lib/actions.test.ts`:

````diff
diff --git a/ui/src/lib/actions.test.ts b/ui/src/lib/actions.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..0b48f5e652bc52866c75e5897a6d29cb8efd9823
--- /dev/null
+++ b/ui/src/lib/actions.test.ts
@@ -0,0 +1,74 @@
+import { describe, expect, it } from 'vitest';
+import { HttpError, type Client } from './api';
+import { ACTIONS, ask, mayAct, noteBody, oneAtATime, outcomeText, recordedAs, VERBS, type Target } from './actions';
+
+const orders: Target = { id: 'orders-pg', node: 'node-b', desired: 'running', actual: 'running', job: null };
+
+describe('pipeline actions', () => {
+  it('are offered to operators and admins, never to viewers', () => {
+    expect([mayAct('viewer'), mayAct('operator'), mayAct('admin')]).toEqual([false, true, true]);
+  });
+
+  it('are offered in the states the server takes them in', () => {
+    const offered = (desired: Target['desired']) => VERBS.filter((v) => ACTIONS[v].takes(desired));
+    expect(offered('running')).toEqual(['flush', 'pause', 'restart', 'stop']);
+    expect(offered('paused')).toEqual(['flush', 'resume', 'stop']);
+    expect(offered('stopped')).toEqual(['resume']);
+  });
+
+  it('say what they do, on which node, and what follows', () => {
+    expect(ACTIONS.restart.effect(orders)).toBe(
+      "orders-pg stops on node-b, drops what it hasn't committed and reads it again from its committed position: nothing is lost or written twice. Lag rises for about 30 s.",
+    );
+    expect(ACTIONS.restart.effect({ ...orders, job: '7c3a9d5e' })).toContain(
+      'Its running snapshot 7c3a9d5e carries on from its last chunk.',
+    );
+    expect(ACTIONS.flush.effect({ ...orders, node: null })).toBe(
+      'orders-pg commits what it has buffered now, whatever its flush policy says.',
+    );
+    expect(ACTIONS.restart.each).toBe(
+      "Each one stops, drops what it hasn't committed and reads it again from its committed position: nothing is lost or written twice. Lag rises for about 30 s on each.",
+    );
+    expect(ACTIONS.stop.destructive).toBe(true);
+    expect(VERBS.filter((v) => ACTIONS[v].destructive)).toEqual(['stop']);
+  });
+
+  it('name the line they will be recorded as', () => {
+    expect(recordedAs('restart', { name: 'Oleksii', role: 'operator' })).toBe('restarted by Oleksii (operator)');
+    expect(recordedAs('pause', { name: 'anonymous', role: 'admin' })).toBe('paused by anonymous (admin)');
+  });
+
+  it('send their note, trimmed, and no body without one', async () => {
+    expect(noteBody('  apply sink.prefix \n')).toEqual({ note: 'apply sink.prefix' });
+    expect(noteBody('   ')).toBeUndefined();
+    const sent: unknown[] = [];
+    const api = { send: async (...a: unknown[]) => sent.push(a) } as unknown as Client;
+    await ask(api, 'restart', 'orders pg', 'why');
+    expect(sent).toEqual([['POST', '/v1/pipelines/orders%20pg/restart', { note: 'why' }]]);
+  });
+});
+
+describe('an action on several pipelines', () => {
+  it('asks one at a time, in order, and stops at the first failure, saying which', async () => {
+    const asked: string[] = [];
+    const o = await oneAtATime(['crm-pg', 'ledger-my', 'payments-pg'], async (id) => {
+      asked.push(id);
+      if (id === 'ledger-my') throw new HttpError(409, 'a paused pipeline restarts once resumed');
+    });
+    expect(asked).toEqual(['crm-pg', 'ledger-my']);
+    expect(o).toEqual({
+      done: ['crm-pg'],
+      failed: { id: 'ledger-my', error: 'a paused pipeline restarts once resumed' },
+      left: ['payments-pg'],
+    });
+    expect(outcomeText(o)).toBe(
+      'ledger-my: a paused pipeline restarts once resumed. Asked already: crm-pg. Not asked: payments-pg.',
+    );
+  });
+
+  it('says nothing more when every one was asked', async () => {
+    const o = await oneAtATime(['a', 'b'], async () => {});
+    expect(o).toEqual({ done: ['a', 'b'], failed: null, left: [] });
+    expect(outcomeText(o)).toBeNull();
+  });
+});
````

- [ ] **Step 2: Run them to see them fail**

Run: `(cd ui && npx vitest run src/lib/actions.test.ts src/lib/Dialog.test.ts)`
Expected: both files fail to load: `Failed to resolve import "./actions"` and `"./Dialog.svelte"`.

- [ ] **Step 3: Implement**

`actions.ts` and `Dialog.svelte`.

`ui/src/lib/Dialog.svelte`:

````diff
diff --git a/ui/src/lib/Dialog.svelte b/ui/src/lib/Dialog.svelte
new file mode 100644
index 0000000000000000000000000000000000000000..128b57dcea961fdf2934ab5493624ae2e50d9834
--- /dev/null
+++ b/ui/src/lib/Dialog.svelte
@@ -0,0 +1,136 @@
+<script lang="ts">
+  // The one dialog every action opens (Admin_Console §5, mockups 05, 06): what it does, to what,
+  // and its effect; an optional note; the line it will be recorded as. It stays open, saying why,
+  // when the action fails.
+  import { onMount, type Snippet } from 'svelte';
+  import { NOTE_MAX } from './actions';
+
+  interface Props {
+    title: string;
+    /** What it does and its effect, a paragraph each. */
+    lines: string[];
+    /** `Recorded as: …`, on each pipeline when there are several. */
+    recorded: string;
+    confirm: string;
+    /** An action that stops something, styled as such. */
+    destructive?: boolean;
+    /** Why it can't be confirmed now (offline, nothing chosen); `null` when it can. */
+    blocked?: string | null;
+    /** Carries the action out: `null` when it's done, else why it failed. */
+    onConfirm: (note: string) => Promise<string | null>;
+    onClose: () => void;
+    /** The action's own fields, above the note. */
+    children?: Snippet;
+  }
+  let {
+    title,
+    lines,
+    recorded,
+    confirm,
+    destructive = false,
+    blocked = null,
+    onConfirm,
+    onClose,
+    children,
+  }: Props = $props();
+
+  let note = $state('');
+  let busy = $state(false);
+  let failed = $state<string | null>(null);
+  let field: HTMLTextAreaElement;
+
+  onMount(() => field.focus());
+
+  async function go() {
+    busy = true;
+    failed = null;
+    const why = await onConfirm(note);
+    busy = false;
+    if (why === null) onClose();
+    else failed = why;
+  }
+
+  function key(e: KeyboardEvent) {
+    if (e.key === 'Escape' && !busy) onClose();
+  }
+</script>
+
+<svelte:window onkeydown={key} />
+<div class="scrim">
+  <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="dialog-title">
+    <h3 id="dialog-title">{title}</h3>
+    {#each lines as l (l)}<p>{l}</p>{/each}
+    {@render children?.()}
+    <label>
+      Note (optional, kept with the event)
+      <textarea bind:this={field} bind:value={note} maxlength={NOTE_MAX} rows="2"></textarea>
+    </label>
+    <p class="muted">Recorded as: {recorded}</p>
+    {#if failed !== null}<p class="failed" role="alert">{failed}</p>{/if}
+    {#if blocked !== null}<p class="muted">{blocked}</p>{/if}
+    <div class="buttons">
+      <button type="button" onclick={onClose} disabled={busy}>Cancel</button>
+      <button type="button" class="go" class:destructive onclick={go} disabled={busy || blocked !== null}>
+        {busy ? `${confirm}…` : confirm}
+      </button>
+    </div>
+  </div>
+</div>
+
+<style>
+  .scrim {
+    position: fixed;
+    inset: 0;
+    background: rgba(0, 0, 0, 0.35);
+    display: flex;
+    align-items: flex-start;
+    justify-content: center;
+    padding-top: 10vh;
+    z-index: 10;
+  }
+  .dialog {
+    width: min(520px, 92vw);
+    background: var(--bg-panel);
+    border: 1px solid var(--border);
+    border-radius: 10px;
+    padding: 16px 18px;
+  }
+  h3 {
+    margin: 0 0 8px;
+    font-size: 15px;
+  }
+  label {
+    display: block;
+    margin-top: 10px;
+    color: var(--text-2);
+  }
+  textarea {
+    display: block;
+    width: 100%;
+    margin-top: 4px;
+    font: inherit;
+    color: var(--text);
+    background: var(--bg);
+    border: 1px solid var(--border);
+    border-radius: 6px;
+    padding: 4px 6px;
+  }
+  .failed {
+    color: var(--red);
+  }
+  .buttons {
+    display: flex;
+    justify-content: flex-end;
+    gap: 8px;
+    margin-top: 12px;
+  }
+  .go {
+    background: var(--accent);
+    border-color: var(--accent);
+    color: #fff;
+  }
+  .go.destructive {
+    background: var(--red);
+    border-color: var(--red);
+  }
+</style>
````

`ui/src/lib/actions.ts`:

````diff
diff --git a/ui/src/lib/actions.ts b/ui/src/lib/actions.ts
new file mode 100644
index 0000000000000000000000000000000000000000..b0ab2c77b218004b1c71a89da9754044c8e7c744
--- /dev/null
+++ b/ui/src/lib/actions.ts
@@ -0,0 +1,139 @@
+// What the console lets a person do to a pipeline (Admin_Console §5): each action's dialog says
+// what it does, to what, and its effect, takes an optional note, and names the line it will be
+// recorded as. The server checks the role again and records the action whatever the page says.
+
+import type { Client } from './api';
+import type { Role, Session } from './session';
+import type { Actual, Desired } from '../model/types';
+
+export type Verb = 'flush' | 'pause' | 'resume' | 'restart' | 'stop';
+/** The order the actions are offered in (mockups 05, 06). */
+export const VERBS: Verb[] = ['flush', 'pause', 'resume', 'restart', 'stop'];
+/** The longest note the server keeps (`MAX_NOTE`). */
+export const NOTE_MAX = 500;
+
+/** A pipeline as an action's dialog describes it. */
+export interface Target {
+  id: string;
+  /** The node that runs it; `null` when none holds its lease. */
+  node: string | null;
+  desired: Desired;
+  actual: Actual | null;
+  /** Its running snapshot job's short id, `null` without one. */
+  job: string | null;
+}
+
+interface Action {
+  label: string;
+  /** As the event names it: `restarted by Oleksii (operator)`. */
+  past: string;
+  /** What it does to each of several, in the list's dialog. */
+  each: string;
+  /** Styled as one that stops something. */
+  destructive: boolean;
+  /** Whether the server takes it in the desired state `desired` (else it answers `409`). */
+  takes: (desired: Desired) => boolean;
+  effect: (t: Target) => string;
+}
+
+const on = (t: Target) => (t.node === null ? '' : ` on ${t.node}`);
+
+export const ACTIONS: Record<Verb, Action> = {
+  flush: {
+    label: 'Flush',
+    past: 'flushed',
+    each: 'Each one commits what it has buffered now, whatever its flush policy says.',
+    destructive: false,
+    takes: (d) => d !== 'stopped',
+    effect: (t) => `${t.id} commits what it has buffered now${on(t)}, whatever its flush policy says.`,
+  },
+  pause: {
+    label: 'Pause',
+    past: 'paused',
+    each: "Each one finishes its open transaction, makes it durable, then reads nothing more; its source connection and its lease stay. Lag grows until it's resumed.",
+    destructive: false,
+    takes: (d) => d === 'running',
+    effect: (t) =>
+      `${t.id} finishes its open transaction${on(t)}, makes it durable, then reads nothing more; its source connection and its lease stay. Lag grows until it's resumed.`,
+  },
+  resume: {
+    label: 'Resume',
+    past: 'resumed',
+    each: 'Each one reads again from its committed position.',
+    destructive: false,
+    takes: (d) => d !== 'running',
+    effect: (t) => `${t.id} reads again from its committed position.`,
+  },
+  restart: {
+    label: 'Restart',
+    past: 'restarted',
+    each: "Each one stops, drops what it hasn't committed and reads it again from its committed position: nothing is lost or written twice. Lag rises for about 30 s on each.",
+    destructive: false,
+    takes: (d) => d === 'running',
+    effect: (t) =>
+      `${t.id} stops${on(t)}, drops what it hasn't committed and reads it again from its committed position: nothing is lost or written twice. Lag rises for about 30 s.` +
+      (t.job === null ? '' : ` Its running snapshot ${t.job} carries on from its last chunk.`),
+  },
+  stop: {
+    label: 'Stop',
+    past: 'stopped',
+    each: "Each one flushes and closes its source connection; its lease stays. The source keeps its log for it until it's resumed: a PostgreSQL slot holds WAL meanwhile.",
+    destructive: true,
+    takes: (d) => d !== 'stopped',
+    effect: (t) =>
+      `${t.id} flushes and closes its source connection${on(t)}; its lease stays. The source keeps its log for it until it's resumed: a PostgreSQL slot holds WAL meanwhile.`,
+  },
+};
+
+/** Whether `role` may act on pipelines: an operator's or an admin's. */
+export function mayAct(role: Role): boolean {
+  return role === 'operator' || role === 'admin';
+}
+
+/** The line an action will be recorded as: `restarted by Oleksii (operator)`. */
+export function recordedAs(verb: Verb, s: Pick<Session, 'name' | 'role'>): string {
+  return `${ACTIONS[verb].past} by ${s.name} (${s.role})`;
+}
+
+/** The body an action sends: its note when there is one. */
+export function noteBody(note: string): { note: string } | undefined {
+  const n = note.trim();
+  return n === '' ? undefined : { note: n };
+}
+
+/** Asks for `verb` on pipeline `id`, with `note`. */
+export async function ask(api: Client, verb: Verb, id: string, note: string): Promise<void> {
+  await api.send('POST', `/v1/pipelines/${encodeURIComponent(id)}/${verb}`, noteBody(note));
+}
+
+/** How a run over several pipelines ended. */
+export interface Outcome {
+  done: string[];
+  /** The one that failed, and why; the run stopped there. */
+  failed: { id: string; error: string } | null;
+  /** Those not asked, after a failure. */
+  left: string[];
+}
+
+/** Runs `each` on `ids` one at a time, in order, stopping at the first failure (mockup 05). */
+export async function oneAtATime(ids: string[], each: (id: string) => Promise<void>): Promise<Outcome> {
+  const done: string[] = [];
+  for (const [i, id] of ids.entries()) {
+    try {
+      await each(id);
+      done.push(id);
+    } catch (e) {
+      return { done, failed: { id, error: e instanceof Error ? e.message : String(e) }, left: ids.slice(i + 1) };
+    }
+  }
+  return { done, failed: null, left: [] };
+}
+
+/** What a run over several pipelines says when it ends: `null` when every one was asked. */
+export function outcomeText(o: Outcome): string | null {
+  if (o.failed === null) return null;
+  const parts = [`${o.failed.id}: ${o.failed.error}.`];
+  if (o.done.length > 0) parts.push(`Asked already: ${o.done.join(', ')}.`);
+  if (o.left.length > 0) parts.push(`Not asked: ${o.left.join(', ')}.`);
+  return parts.join(' ');
+}
````

- [ ] **Step 4: Run the tests and the gates**

Run the step's tests again, then

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
git add -A ui/src/lib/Dialog.svelte ui/src/lib/Dialog.test.ts ui/src/lib/actions.test.ts ui/src/lib/actions.ts
git commit -m "feat(ui): the pipeline actions and their dialog — effect, note, recorded as; several one at a time"
```

---

### Task 4: The pipeline list (`ui/src/pipelines/{rows.ts,lag.svelte.ts,List.svelte}`, the router)

**Files:**
- Modify: `ui/src/App.svelte`
- Modify: `ui/src/lib/router.svelte.ts`
- Modify: `ui/src/lib/router.test.ts`
- Modify: `ui/src/model/fixtures.ts`
- Modify: `ui/src/model/types.ts`
- Create: `ui/src/pipelines/List.svelte`
- Create: `ui/src/pipelines/List.test.ts`
- Create: `ui/src/pipelines/lag.svelte.ts`
- Create: `ui/src/pipelines/lag.test.ts`
- Create: `ui/src/pipelines/rows.test.ts`
- Create: `ui/src/pipelines/rows.ts`

**Interfaces:**
- Consumes: Task 1's `Model`, `route`, `facts()`; Task 2's `formatValue`; Task 3's `ACTIONS`, `ask`,
  `mayAct`, `oneAtATime`, `outcomeText`, `recordedAs`, `VERBS`, `Dialog`; M15's `rows()`
  (`overview/derive.ts`), `attention()`, `shortJob()`, `LAG_TARGET_S`, `Sparkline`.
- Produces (`ui/src/lib/router.svelte.ts`, D-M16a.9): `restOf(path): string[]`; `Router.rest`,
  `Router.query: URLSearchParams`, `Router.replace(to)`.
- Produces (`ui/src/pipelines/lag.svelte.ts`): `SPAN_S` (6 h), `STEP_S` (60); `class
  LagHistory(api, pipeline: string | null = null, now = Date.now)` with `lines: Record<string,
  [number, number][]>`, `error: string | null`, `ensure()`, `load()`, `follow(live)`, `sample(m)`.
- Produces (`ui/src/pipelines/rows.ts`, D-M16a.6): `type Show`, `SHOWS`, `type Column`, `interface
  ListQuery { show; node; source; text; sort; desc }`, `parseQuery(q)`, `queryString(l)`,
  `sortBy(l, column)`, `interface ListRow extends Row { order; name; route; source; desired; actual;
  away; changes; tables; halted; snapshot; job }`, `pipelineOf(href)`, `listRows(f, lines, now)`,
  `needingAttention(f, now): Set<string>`, `counts(all, flagged)`, `view(all, l, flagged)`.
- Produces (`ui/src/pipelines/List.svelte`): props `model`, `lag`, `session`, `api`, `router`,
  `now`, `stale`, `last`, `offline`.
- Produces (`model/types.ts`): `TableItem` with the table's figures (`snapshot`,
  `changes_per_minute`, `version`, `files_rewritten`, `files`, `soft_dropped`, `fresh_as_of`,
  `stale`); `Job.eta_seconds`; `fixtures.ts`'s `table(pipeline, name, over)`.

- [ ] **Step 1: Write the failing tests**

The types gain the tables' figures and the jobs' ETA, and the fixtures build their tables with
`table()`. The tests: `a page's address` (`router.test.ts`: the names after the section; the
query changed in place); `the pipeline list's rows` (`rows.test.ts`: a row a pipeline with what a
person scans for; the 6 hours' lines; the counts; filters and sorts; the address read back;
the attention lines' pipelines); `pipelines' lag over 6 hours` (`lag.test.ts`); `the pipeline
list` (`List.test.ts`: worst first with route, node, lag and tables; the view its address names,
changed in place; a row opened by a click, ↑ and ↓; nothing to select for a viewer; several acted
on, stopping at the first failure; actions waiting while offline).

`ui/src/lib/router.test.ts`:

````diff
diff --git a/ui/src/lib/router.test.ts b/ui/src/lib/router.test.ts
index d76a7ccd02e81d7d4c9f9c3a24ee8bf9e19bbbc5..b8b0ec4940e1d5806021cbb7935e313b33ccbf6a 100644
--- a/ui/src/lib/router.test.ts
+++ b/ui/src/lib/router.test.ts
@@ -1,5 +1,5 @@
 import { describe, expect, it } from 'vitest';
-import { isAppLink, sectionOf } from './router.svelte';
+import { isAppLink, restOf, Router, sectionOf } from './router.svelte';
 
 describe('the pages', () => {
   it('are named by their first name under /ui/', () => {
@@ -26,3 +26,27 @@ describe('the pages', () => {
     expect(isAppLink(link('https://example.com/ui/'), click())).toBe(false);
   });
 });
+
+describe("a page's address", () => {
+  it('names the object after the section, decoded', () => {
+    expect(restOf('/ui/pipelines')).toEqual([]);
+    expect(restOf('/ui/pipelines/orders%20pg?show=halted')).toEqual(['orders pg']);
+    expect(restOf('/ui/tables/orders-pg/public.refunds')).toEqual(['orders-pg', 'public.refunds']);
+    expect(restOf('/ui/pipelines/%E0%A4')).toEqual(['%E0%A4']);
+  });
+
+  it("keeps a list's filters in its query, changed in place so Back leaves the list", () => {
+    history.replaceState(null, '', '/ui/');
+    const r = new Router();
+    r.go('/ui/pipelines');
+    const before = history.length;
+    r.replace('/ui/pipelines?show=halted&sort=lag');
+    expect(location.pathname + location.search).toBe('/ui/pipelines?show=halted&sort=lag');
+    expect(history.length).toBe(before);
+    expect(r.query.get('show')).toBe('halted');
+    expect(r.rest).toEqual([]);
+    r.go('/ui/pipelines/orders-pg');
+    expect(r.rest).toEqual(['orders-pg']);
+    expect(r.section).toBe('pipelines');
+  });
+});
````

`ui/src/model/fixtures.ts`:

````diff
diff --git a/ui/src/model/fixtures.ts b/ui/src/model/fixtures.ts
index 6bbd6f0feace1489f38eba34e6034f2355b277de..2408e0ca8845c730c323faee9eedeaa39c47e586 100644
--- a/ui/src/model/fixtures.ts
+++ b/ui/src/model/fixtures.ts
@@ -1,7 +1,7 @@
 // The tests' cluster: mockup 01's, in the API's shapes.
 
 import type { Facts } from './facts';
-import type { Actual, ClusterStatus, Desired, EventItem, Figures, PipelineItem, SourceAt } from './types';
+import type { Actual, ClusterStatus, Desired, EventItem, Figures, PipelineItem, SourceAt, TableItem } from './types';
 
 export const NOW = Date.UTC(2026, 10, 25, 14, 2, 0);
 const MIN = 60_000;
@@ -57,6 +57,43 @@ function lease(id: string, holder: string, owner_epoch: number, ring_owner: stri
   return { id, holder, holder_stale: false, owner_epoch, expires_at: iso(NOW + 8_000), ring_owner };
 }
 
+/** A table with nothing to say but what `over` says: streaming, no figures yet. */
+export function table(pipeline: string, name: string, over: Partial<TableItem> = {}): TableItem {
+  return {
+    pipeline,
+    table: name,
+    state: 'streaming',
+    halt: null,
+    snapshot: null,
+    changes_per_minute: 0,
+    last_commit: null,
+    version: null,
+    files_rewritten: null,
+    files: null,
+    soft_dropped: 0,
+    fresh_as_of: null,
+    stale: false,
+    ...over,
+  };
+}
+
+/** A table as its pipeline's figures show it. */
+const live = ({ pipeline: _p, ...t }: TableItem) => t;
+
+const refunds = table('orders-pg', 'public.refunds', {
+  state: 'halted',
+  halt: { code: 'SCHEMA_REFUSED', detail: "amount changed int → text, which can't be applied in place", since: iso(NOW - 12 * MIN) },
+  last_commit: iso(NOW - 15 * MIN),
+  version: 2210,
+});
+const orders = table('orders-pg', 'public.orders', {
+  changes_per_minute: 1840,
+  last_commit: iso(NOW - 8_000),
+  version: 18044,
+  files_rewritten: 6,
+  files: 820,
+});
+
 export function facts(): Facts {
   return {
     pipelines: [
@@ -85,15 +122,15 @@ export function facts(): Facts {
       ],
     },
     jobs: [
-      { job_id: 'customers~019ab5d07c3a7d5e8f0a1b2c3d4e5f60', pipeline: 'customers', status: 'running', rows_done: 62, estimate: 100, requested_at: iso(NOW - 20 * MIN), finished_at: null, error: null },
-      { job_id: 'billing-mysql~019ab4f25b1e7f40a3c2d1e0f9a8b7c6', pipeline: 'billing-mysql', status: 'failed', rows_done: 10, estimate: 50, requested_at: iso(NOW - 200 * MIN), finished_at: iso(NOW - 180 * MIN), error: 'the source connection was lost' },
+      { job_id: 'customers~019ab5d07c3a7d5e8f0a1b2c3d4e5f60', pipeline: 'customers', status: 'running', rows_done: 62, estimate: 100, eta_seconds: 420, requested_at: iso(NOW - 20 * MIN), finished_at: null, error: null },
+      { job_id: 'billing-mysql~019ab4f25b1e7f40a3c2d1e0f9a8b7c6', pipeline: 'billing-mysql', status: 'failed', rows_done: 10, estimate: 50, eta_seconds: null, requested_at: iso(NOW - 200 * MIN), finished_at: iso(NOW - 180 * MIN), error: 'the source connection was lost' },
     ],
     tables: [
-      { pipeline: 'orders-pg', table: 'public.refunds', state: 'halted', halt: { code: 'SCHEMA_REFUSED', detail: "amount changed int → text, which can't be applied in place", since: iso(NOW - 12 * MIN) }, last_commit: iso(NOW - 15 * MIN) },
-      { pipeline: 'orders-pg', table: 'public.orders', state: 'streaming', halt: null, last_commit: iso(NOW - 8_000) },
-      { pipeline: 'billing-mysql', table: 'billing.invoices', state: 'streaming', halt: null, last_commit: iso(NOW - 3 * MIN) },
-      { pipeline: 'inventory', table: 'public.items', state: 'streaming', halt: null, last_commit: iso(NOW - 120 * MIN) },
-      { pipeline: 'customers', table: 'public.customers', state: 'snapshot', halt: null, last_commit: iso(NOW - 12_000) },
+      refunds,
+      orders,
+      table('billing-mysql', 'billing.invoices', { last_commit: iso(NOW - 3 * MIN) }),
+      table('inventory', 'public.items', { last_commit: iso(NOW - 120 * MIN) }),
+      table('customers', 'public.customers', { state: 'snapshot', snapshot: { rows_done: 62, waiting: false }, last_commit: iso(NOW - 12_000) }),
     ],
     figures: {
       'billing-mysql': figures({
@@ -101,7 +138,7 @@ export function facts(): Facts {
         node: 'n1',
         owner_epoch: 3,
         source: { detail: { engine: 'mysql' } },
-        tables: [{ table: 'billing.invoices', state: 'streaming', halt: null, last_commit: iso(NOW - 3 * MIN) }],
+        tables: [live(table('billing-mysql', 'billing.invoices', { changes_per_minute: 2210, last_commit: iso(NOW - 3 * MIN) }))],
         worker: { lag_seconds: 252, degraded: null },
       }),
       inventory: figures({
@@ -110,17 +147,14 @@ export function facts(): Facts {
         actual: 'paused',
         owner_epoch: 1,
         source: { detail: { engine: 'postgres', slot: { name: 'da_inventory', wal_held_bytes: 3.2 * GIB }, max_slot_wal_keep_size: 4 * GIB } },
-        tables: [{ table: 'public.items', state: 'streaming', halt: null, last_commit: iso(NOW - 120 * MIN) }],
+        tables: [live(table('inventory', 'public.items', { last_commit: iso(NOW - 120 * MIN) }))],
         worker: { lag_seconds: null, degraded: null },
       }),
       'orders-pg': figures({
         id: 'orders-pg',
         node: 'n2',
         source: { detail: { engine: 'postgres', slot: { name: 'da_orders', wal_held_bytes: 0.2 * GIB }, max_slot_wal_keep_size: null } },
-        tables: [
-          { table: 'public.refunds', state: 'halted', halt: { code: 'SCHEMA_REFUSED', detail: "amount changed int → text, which can't be applied in place", since: iso(NOW - 12 * MIN) }, last_commit: iso(NOW - 15 * MIN) },
-          { table: 'public.orders', state: 'streaming', halt: null, last_commit: iso(NOW - 8_000) },
-        ],
+        tables: [live(refunds), live(orders)],
         worker: { lag_seconds: 21, degraded: null },
       }),
     },
````

`ui/src/model/types.ts`:

````diff
diff --git a/ui/src/model/types.ts b/ui/src/model/types.ts
index ca882d143492edc1a883cbdb77fd7817a797fb5a..01af0c8afc08dd2353574bf8815a04a571337d45 100644
--- a/ui/src/model/types.ts
+++ b/ui/src/model/types.ts
@@ -70,9 +70,23 @@ export interface Halt {
 export interface TableItem {
   pipeline: string;
   table: string;
+  /** `streaming`, `halted`, `snapshot` or `copied`. */
   state: string;
   halt: Halt | null;
+  snapshot: { rows_done: number; waiting: boolean } | null;
+  /** The stream's changes committed in the last minute. */
+  changes_per_minute: number;
   last_commit: string | null;
+  /** The Delta table's version after the last commit. */
+  version: number | null;
+  /** The last MERGE's files rewritten, of the table's `files`. */
+  files_rewritten: number | null;
+  files: number | null;
+  /** The sink's columns the source no longer has, kept and written NULL. */
+  soft_dropped: number;
+  /** What lake readers see: the source time of the newest change committed. */
+  fresh_as_of: string | null;
+  stale: boolean;
 }
 
 /** `GET /v1/tables`. */
@@ -90,6 +104,8 @@ export interface Job {
   status: 'queued' | 'running' | 'cancelling' | 'completed' | 'failed' | 'cancelled';
   rows_done: number;
   estimate: number | null;
+  /** Seconds left at the observed rate, `null` until there is one. */
+  eta_seconds: number | null;
   requested_at: string;
   finished_at: string | null;
   error: string | null;
````

`ui/src/pipelines/List.test.ts`:

````diff
diff --git a/ui/src/pipelines/List.test.ts b/ui/src/pipelines/List.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..29f7046645bcc9d11183823072596d621a4e2ee8
--- /dev/null
+++ b/ui/src/pipelines/List.test.ts
@@ -0,0 +1,154 @@
+import { flushSync, mount, tick, unmount } from 'svelte';
+import { afterEach, describe, expect, it } from 'vitest';
+import { HttpError, type Client } from '../lib/api';
+import { Router } from '../lib/router.svelte';
+import type { Role, Session } from '../lib/session';
+import { facts, NOW } from '../model/fixtures';
+import { Model } from '../model/model.svelte';
+import { LagHistory } from './lag.svelte';
+import List from './List.svelte';
+
+let mounted: ReturnType<typeof mount> | undefined;
+afterEach(() => {
+  if (mounted !== undefined) unmount(mounted);
+  mounted = undefined;
+});
+
+/** A client whose actions answer from `fail` (a pipeline's error), and record what was sent. */
+function api(fail: Record<string, HttpError> = {}): Client & { sent: unknown[][] } {
+  const sent: unknown[][] = [];
+  return {
+    sent,
+    get: async <T,>() => ({ series: [] }) as T,
+    send: async <T,>(method: string, path: string, body?: unknown) => {
+      sent.push([method, path, body]);
+      const id = decodeURIComponent(path.split('/')[3]);
+      if (id in fail) throw fail[id];
+      return {} as T;
+    },
+  };
+}
+
+const session = (role: Role): Session => ({ name: 'Oleksii', role, auth: 'basic', expires: null, signed_in: null, csrf: 't' });
+
+function render(over: { role?: Role; path?: string; client?: Client; offline?: boolean } = {}) {
+  history.replaceState(null, '', over.path ?? '/ui/pipelines');
+  const router = new Router();
+  const client = over.client ?? api();
+  const model = new Model(client);
+  model.facts = facts();
+  const target = document.createElement('div');
+  document.body.replaceChildren(target);
+  mounted = mount(List, {
+    target,
+    props: {
+      model,
+      lag: new LagHistory(client, null, () => NOW),
+      session: session(over.role ?? 'operator'),
+      api: client,
+      router,
+      now: NOW,
+      stale: false,
+      last: NOW,
+      offline: over.offline ?? false,
+    },
+  });
+  flushSync();
+  return { t: target, router };
+}
+
+const ids = (t: HTMLElement) => [...t.querySelectorAll('tbody a.open')].map((a) => a.textContent);
+const settle = async () => {
+  for (let i = 0; i < 5; i++) await tick();
+  flushSync();
+};
+
+function check(t: HTMLElement, id: string) {
+  const box = t.querySelector<HTMLInputElement>(`input[aria-label="Select ${id}"]`)!;
+  box.click();
+  flushSync();
+}
+
+function button(t: HTMLElement, text: string): HTMLButtonElement {
+  return [...t.querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent?.trim() === text)!;
+}
+
+describe('the pipeline list', () => {
+  it('lists every pipeline worst first, with its route, node, lag against the target and snapshot', () => {
+    const { t } = render();
+    expect(ids(t)).toEqual(['orders-pg', 'billing-mysql', 'customers', 'inventory']);
+    const first = t.querySelector('tbody tr')!;
+    expect(first.textContent).toContain('PG orders@pg-prod-replica → Δ lake/orders-pg');
+    expect(first.textContent).toContain('n2 away');
+    expect(first.textContent).toContain('21 s / 60 s');
+    expect(first.textContent).toContain('1 of 2');
+    expect(t.querySelector('.chips [aria-current="true"]')?.textContent).toContain('All 4');
+  });
+
+  it('shows the view its address names, and changes the address in place', () => {
+    const { t, router } = render({ path: '/ui/pipelines?show=attention&sort=pipeline' });
+    expect(ids(t)).toEqual(['billing-mysql', 'inventory', 'orders-pg']);
+    const before = history.length;
+    [...t.querySelectorAll<HTMLAnchorElement>('.chips a')].find((a) => a.textContent?.includes('Paused'))!.click();
+    flushSync();
+    expect(router.path).toBe('/ui/pipelines?show=paused&sort=pipeline');
+    expect(history.length).toBe(before);
+    expect(ids(t)).toEqual(['inventory']);
+  });
+
+  it('opens a row from a click anywhere on it, and moves between rows with ↑ and ↓', () => {
+    const { t, router } = render();
+    const rows = t.querySelectorAll('tbody tr');
+    (rows[1].querySelector('td:nth-child(4)') as HTMLElement).click();
+    flushSync();
+    expect(router.path).toBe('/ui/pipelines/billing-mysql');
+    router.go('/ui/pipelines');
+    flushSync();
+    const links = t.querySelectorAll<HTMLAnchorElement>('tbody a.open');
+    links[0].focus();
+    links[0].dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true }));
+    expect(document.activeElement).toBe(links[1]);
+    links[1].dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowUp', bubbles: true }));
+    expect(document.activeElement).toBe(links[0]);
+  });
+
+  it("offers a viewer nothing to select or act on", () => {
+    const { t } = render({ role: 'viewer' });
+    expect(t.querySelectorAll('input[type="checkbox"]')).toHaveLength(0);
+  });
+
+  it('acts on several, one at a time, naming those the action skips, and stops at the first failure', async () => {
+    const client = api({ 'billing-mysql': new HttpError(409, 'no node holds its lease') });
+    const { t } = render({ client });
+    for (const id of ['orders-pg', 'billing-mysql', 'inventory', 'customers']) check(t, id);
+    expect(t.querySelector('.acting')?.textContent).toContain('4 selected');
+    button(t, 'Restart…').click();
+    flushSync();
+    const dialog = t.querySelector('[role="dialog"]')!;
+    expect(dialog.querySelector('h3')?.textContent).toBe('Restart 3 pipelines?');
+    expect(dialog.textContent).toContain('orders-pg on n2 · billing-mysql on n1 · customers on n1');
+    expect(dialog.textContent).toContain("customers's running snapshot 019ab5d0 carries on from its last chunk after the restart.");
+    expect(dialog.textContent).toContain("Not asked, restart doesn't apply: inventory (paused).");
+    expect(dialog.textContent).toContain('Recorded as: restarted by Oleksii (operator), on each pipeline');
+    const area = dialog.querySelector('textarea')!;
+    area.value = 'rotate the TLS certificate';
+    area.dispatchEvent(new Event('input'));
+    button(t, 'Restart 3').click();
+    await settle();
+    expect(client.sent).toEqual([
+      ['POST', '/v1/pipelines/orders-pg/restart', { note: 'rotate the TLS certificate' }],
+      ['POST', '/v1/pipelines/billing-mysql/restart', { note: 'rotate the TLS certificate' }],
+    ]);
+    expect(t.querySelector('[role="dialog"] [role="alert"]')?.textContent).toBe(
+      'billing-mysql: no node holds its lease. Asked already: orders-pg. Not asked: customers.',
+    );
+    // What was asked is no longer selected.
+    expect(t.querySelector('.acting')?.textContent).toContain('3 selected');
+  });
+
+  it('keeps actions from being confirmed while offline', () => {
+    const { t } = render({ offline: true });
+    check(t, 'orders-pg');
+    expect(button(t, 'Flush…').disabled).toBe(true);
+  });
+});
````

`ui/src/pipelines/lag.test.ts`:

````diff
diff --git a/ui/src/pipelines/lag.test.ts b/ui/src/pipelines/lag.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..07a302473f64d04353d807cb9174cf1bb18449d6
--- /dev/null
+++ b/ui/src/pipelines/lag.test.ts
@@ -0,0 +1,60 @@
+import { describe, expect, it } from 'vitest';
+import { HttpError, type Client } from '../lib/api';
+import { NOW } from '../model/fixtures';
+import { LagHistory, SPAN_S } from './lag.svelte';
+
+function api(answer: unknown | Error): Client & { asked: string[] } {
+  const asked: string[] = [];
+  return {
+    asked,
+    get: async <T,>(path: string) => {
+      asked.push(path);
+      if (answer instanceof Error) throw answer;
+      return answer as T;
+    },
+    send: async () => {
+      throw new Error('reads only');
+    },
+  };
+}
+
+const t = NOW / 1000;
+
+describe("pipelines' lag over 6 hours", () => {
+  it('reads every line, or one pipeline\'s, at a minute a point', async () => {
+    const all = api({ series: [{ name: 'lag_seconds', pipeline: 'orders-pg', points: [[t - 60, 3]] }] });
+    const h = new LagHistory(all, null, () => NOW);
+    await h.load();
+    const since = new Date(NOW - SPAN_S * 1000).toISOString();
+    expect(all.asked).toEqual([`/v1/history?series=lag_seconds&since=${since}&step=60`]);
+    expect(h.lines).toEqual({ 'orders-pg': [[t - 60, 3]] });
+    const one = api({ series: [] });
+    await new LagHistory(one, 'orders pg', () => NOW).load();
+    expect(one.asked).toEqual([`/v1/history?series=lag_seconds&pipeline=orders%20pg&since=${since}&step=60`]);
+  });
+
+  it('reads once however often it is asked to be there', async () => {
+    const a = api({ series: [] });
+    const h = new LagHistory(a, null, () => NOW);
+    await h.ensure();
+    await h.ensure();
+    expect(a.asked).toHaveLength(1);
+  });
+
+  it('adds each sample and keeps 6 hours; one scoped to a pipeline takes its own alone', () => {
+    const h = new LagHistory(api({ series: [] }), 'orders-pg', () => NOW);
+    h.lines = { 'orders-pg': [[t - SPAN_S, 1], [t - 60, 2]] };
+    h.sample({ scope: 'pipeline', pipeline: 'orders-pg', at: t, series: { lag_seconds: 4 } });
+    h.sample({ scope: 'pipeline', pipeline: 'crm-pg', at: t, series: { lag_seconds: 94 } });
+    h.sample({ scope: 'node', at: t, series: { memory_bytes: 1 } });
+    expect(h.lines).toEqual({ 'orders-pg': [[t - 60, 2], [t, 4]] });
+  });
+
+  it('keeps its lines and says why when a read fails', async () => {
+    const h = new LagHistory(api(new HttpError(503, 'the chart history: slow down')), null, () => NOW);
+    h.lines = { 'orders-pg': [[t, 1]] };
+    await h.load();
+    expect(h.error).toBe('the chart history: slow down');
+    expect(h.lines).toEqual({ 'orders-pg': [[t, 1]] });
+  });
+});
````

`ui/src/pipelines/rows.test.ts`:

````diff
diff --git a/ui/src/pipelines/rows.test.ts b/ui/src/pipelines/rows.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..04817b1dbce7f7f7b0484a22d31618308597cfa7
--- /dev/null
+++ b/ui/src/pipelines/rows.test.ts
@@ -0,0 +1,93 @@
+import { describe, expect, it } from 'vitest';
+import { facts, NOW } from '../model/fixtures';
+import {
+  counts,
+  listRows,
+  needingAttention,
+  parseQuery,
+  pipelineOf,
+  queryString,
+  sortBy,
+  view,
+  type ListQuery,
+} from './rows';
+
+const all = () => listRows(facts(), {}, NOW);
+const base: ListQuery = { show: 'all', node: null, source: null, text: '', sort: null, desc: false };
+
+describe("the pipeline list's rows", () => {
+  it('are a pipeline each, worst first, with what a person scans for', () => {
+    const rows = all();
+    expect(rows.map((r) => r.id)).toEqual(['orders-pg', 'billing-mysql', 'customers', 'inventory']);
+    const [orders, billing, customers, inventory] = rows;
+    expect(orders).toMatchObject({
+      route: 'PG orders@pg-prod-replica → Δ lake/orders-pg',
+      node: 'n2',
+      away: true,
+      changes: 1840,
+      tables: 2,
+      halted: 1,
+      snapshot: null,
+    });
+    expect(orders.state).toEqual({ label: '1 halted', tone: 'red', note: null });
+    expect(billing).toMatchObject({ away: false, changes: 2210, snapshot: '019ab4f2 ✕ failed 3 h ago', job: null });
+    // No live figures yet: its changes aren't known.
+    expect(customers).toMatchObject({ changes: null, snapshot: '62% · ETA 7m 0s', job: '019ab5d0' });
+    expect(inventory).toMatchObject({ desired: 'paused', actual: 'paused', changes: 0 });
+  });
+
+  it("draw each lag line from the last 6 hours' when they have them", () => {
+    const line: [number, number][] = [
+      [NOW / 1000 - 6 * 3600, 3],
+      [NOW / 1000, 21],
+    ];
+    expect(listRows(facts(), { 'orders-pg': line }, NOW)[0].points).toEqual(line);
+  });
+
+  it('are counted for each filter; "Needs attention" names those its lines are about', () => {
+    const flagged = needingAttention(facts(), NOW);
+    expect([...flagged].sort()).toEqual(['billing-mysql', 'inventory', 'orders-pg']);
+    expect(counts(all(), flagged)).toEqual({
+      all: 4,
+      attention: 3,
+      running: 3,
+      halted: 1,
+      paused: 1,
+      stopped: 0,
+      snapshot: 1,
+    });
+  });
+
+  it('are filtered by state, node, source and name, worst first unless a column sorts them', () => {
+    const flagged = needingAttention(facts(), NOW);
+    const ids = (q: Partial<ListQuery>) => view(all(), { ...base, ...q }, flagged).map((r) => r.id);
+    expect(ids({ show: 'attention' })).toEqual(['orders-pg', 'billing-mysql', 'inventory']);
+    expect(ids({ node: 'n1' })).toEqual(['billing-mysql', 'customers']);
+    expect(ids({ source: 'mysql' })).toEqual(['billing-mysql']);
+    expect(ids({ text: 'ORD' })).toEqual(['orders-pg']);
+    expect(ids({ sort: 'lag', desc: true })).toEqual(['billing-mysql', 'orders-pg', 'customers', 'inventory']);
+    expect(ids({ sort: 'pipeline' })).toEqual(['billing-mysql', 'customers', 'inventory', 'orders-pg']);
+  });
+
+  it('are sorted by a column biggest first when it is a number, the other way on a second click', () => {
+    expect(sortBy(base, 'lag')).toMatchObject({ sort: 'lag', desc: true });
+    expect(sortBy({ ...base, sort: 'lag', desc: true }, 'lag')).toMatchObject({ sort: 'lag', desc: false });
+    expect(sortBy(base, 'pipeline')).toMatchObject({ sort: 'pipeline', desc: false });
+  });
+
+  it('keep their view in the address, read back as written', () => {
+    const q: ListQuery = { show: 'halted', node: 'n2', source: 'postgres', text: 'ord', sort: 'lag', desc: true };
+    expect(queryString(q)).toBe('?show=halted&node=n2&source=postgres&q=ord&sort=lag&desc');
+    expect(parseQuery(new URLSearchParams(queryString(q)))).toEqual(q);
+    expect(queryString(base)).toBe('');
+    expect(parseQuery(new URLSearchParams('show=nonsense&sort=nope&source=oracle'))).toEqual(base);
+  });
+
+  it('find the pipeline a "Needs attention" line links to', () => {
+    expect(pipelineOf('/ui/pipelines/orders%20pg')).toBe('orders pg');
+    expect(pipelineOf('/ui/tables/orders-pg/public.refunds')).toBe('orders-pg');
+    expect(pipelineOf('/ui/snapshots/billing-mysql~019ab4f2')).toBe('billing-mysql');
+    expect(pipelineOf('/ui/config')).toBeNull();
+    expect(pipelineOf('/ui/cluster/nodes/n1')).toBeNull();
+  });
+});
````

- [ ] **Step 2: Run them to see them fail**

Run: `(cd ui && npm test)`
Expected: `rows.test.ts` fails to load (`Failed to resolve import "./rows"`), `lag.test.ts` and
`List.test.ts` too (`"./lag.svelte"`); `router.test.ts`'s two new tests fail (`restOf is not a
function`, `r.replace is not a function`); the other files pass (83 tests).

- [ ] **Step 3: Implement**

The router's `rest`, `query` and `replace`; `rows.ts`, `lag.svelte.ts`, `List.svelte`; `App.svelte`
shows the list at `/ui/pipelines`, with one `LagHistory` read when it is first shown.

`ui/src/App.svelte`:

````diff
diff --git a/ui/src/App.svelte b/ui/src/App.svelte
index d51c1870ab4e056570c69257066ec092efba646d..ef3a09e80d87b55640caa4d1b6d56ba41db2ef96 100644
--- a/ui/src/App.svelte
+++ b/ui/src/App.svelte
@@ -9,6 +9,8 @@
   import { awaitSession, checkSession, keepFresh, RETRY_MS, type Session } from './lib/session';
   import { Model } from './model/model.svelte';
   import Overview from './overview/Overview.svelte';
+  import { LagHistory } from './pipelines/lag.svelte';
+  import List from './pipelines/List.svelte';
 
   // The server says why the session ended: its 401 or 403 page, or the sign-in.
   const ended = () => location.reload();
@@ -26,6 +28,8 @@
     check: () => checkSession({ fetch, ended }),
   });
   const model = new Model(api);
+  /** Every pipeline's lag over 6 hours, read when the list is first shown. */
+  const lag = new LagHistory(api);
 
   async function readGeneration() {
     try {
@@ -43,6 +47,7 @@
         if ((e as { kind?: string }).kind === 'config.published') void readGeneration();
       }),
       model.follow(live),
+      lag.follow(live),
     ];
     const read = (s: Session) => {
       session = s;
@@ -77,6 +82,18 @@
   >
     {#if router.section === 'overview'}
       <Overview {model} {now} stale={live.stale(now)} last={live.last} />
+    {:else if router.section === 'pipelines' && router.rest.length === 0}
+      <List
+        {model}
+        {lag}
+        {session}
+        {api}
+        {router}
+        {now}
+        stale={live.stale(now)}
+        last={live.last}
+        offline={live.state === 'offline'}
+      />
     {:else}
       <Later section={router.section} />
     {/if}
````

`ui/src/lib/router.svelte.ts`:

````diff
diff --git a/ui/src/lib/router.svelte.ts b/ui/src/lib/router.svelte.ts
index 1b46e854d1b5f46211d278acb66a54a3921be526..cc9800715308c012fa0b5cf27344d124203b66de 100644
--- a/ui/src/lib/router.svelte.ts
+++ b/ui/src/lib/router.svelte.ts
@@ -23,6 +23,18 @@ export function sectionOf(path: string): Section {
   return SECTIONS.find((s) => s.id === first)?.id ?? 'overview';
 }
 
+/** The names after a page's section, decoded: `['orders-pg']` for `/ui/pipelines/orders-pg`. */
+export function restOf(path: string): string[] {
+  const names = path.split(/[?#]/)[0].replace(/^\/ui\/?/, '').split('/').filter((n) => n !== '');
+  return names.slice(1).map((n) => {
+    try {
+      return decodeURIComponent(n);
+    } catch {
+      return n;
+    }
+  });
+}
+
 /** Whether a click on `a` is the app's to handle: a plain left click on one of its pages. */
 export function isAppLink(a: HTMLAnchorElement, e: MouseEvent): boolean {
   if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) {
@@ -37,6 +49,10 @@ export function isAppLink(a: HTMLAnchorElement, e: MouseEvent): boolean {
 export class Router {
   path = $state(location.pathname + location.search);
   section = $derived(sectionOf(this.path));
+  /** The names after the section: an object's page. */
+  rest = $derived(restOf(this.path));
+  /** The address's query: a list's filters and sort. */
+  query = $derived(new URLSearchParams(this.path.split('?')[1] ?? ''));
 
   /** Follows the app's links, and Back and Forward; returns the way to stop. */
   listen(): () => void {
@@ -61,4 +77,11 @@ export class Router {
     this.path = to;
     window.scrollTo(0, 0);
   }
+
+  /** Changes the address in place (a list's filters): Back goes to the page before the list. */
+  replace(to: string): void {
+    if (to === this.path) return;
+    history.replaceState(null, '', to);
+    this.path = to;
+  }
 }
````

`ui/src/pipelines/List.svelte`:

````diff
diff --git a/ui/src/pipelines/List.svelte b/ui/src/pipelines/List.svelte
new file mode 100644
index 0000000000000000000000000000000000000000..7a0c536f94ba5c6bdc7526684df900e68a5323f3
--- /dev/null
+++ b/ui/src/pipelines/List.svelte
@@ -0,0 +1,406 @@
+<script lang="ts">
+  // The pipeline list (Admin_Console §4, mockup 05): worst first; filters by state, node, source
+  // and name, and a column's sort, all in the address; ↑/↓ and Enter move and open; select
+  // several to act on them together, one at a time, stopping at the first failure.
+  import { onMount } from 'svelte';
+  import type { Client } from '../lib/api';
+  import {
+    ACTIONS,
+    ask,
+    mayAct,
+    oneAtATime,
+    outcomeText,
+    recordedAs,
+    VERBS,
+    type Verb,
+  } from '../lib/actions';
+  import Dialog from '../lib/Dialog.svelte';
+  import { ago, clock, duration } from '../lib/format';
+  import type { Router } from '../lib/router.svelte';
+  import { formatValue } from '../lib/series';
+  import type { Session } from '../lib/session';
+  import type { Model } from '../model/model.svelte';
+  import { LAG_TARGET_S } from '../overview/attention';
+  import Sparkline from '../overview/Sparkline.svelte';
+  import type { LagHistory } from './lag.svelte';
+  import {
+    counts,
+    listRows,
+    needingAttention,
+    parseQuery,
+    queryString,
+    SHOWS,
+    sortBy,
+    view,
+    type Column,
+    type ListQuery,
+    type ListRow,
+  } from './rows';
+
+  interface Props {
+    model: Model;
+    lag: LagHistory;
+    session: Session;
+    api: Client;
+    router: Router;
+    now: number;
+    /** The live updates stopped: every number is as of `last`. */
+    stale: boolean;
+    last: number | null;
+    /** Offline: actions wait. */
+    offline: boolean;
+  }
+  let { model, lag, session, api, router, now, stale, last, offline }: Props = $props();
+
+  const q = $derived(parseQuery(router.query));
+  const all = $derived(listRows(model.facts, lag.lines, now));
+  const flagged = $derived(needingAttention(model.facts, now));
+  const shown = $derived(view(all, q, flagged));
+  const n = $derived(counts(all, flagged));
+  const nodes = $derived((model.facts.cluster?.members ?? []).map((m) => m.node).sort());
+  const can = $derived(mayAct(session.role));
+
+  let selected = $state<string[]>([]);
+  let acting = $state<Verb | null>(null);
+
+  const chosen = $derived(all.filter((r) => selected.includes(r.id)));
+  const offered = $derived(VERBS.filter((v) => chosen.some((r) => ACTIONS[v].takes(r.desired))));
+
+  const href = (id: string) => `/ui/pipelines/${encodeURIComponent(id)}`;
+  const at = (l: ListQuery) => `/ui/pipelines${queryString(l)}`;
+  const set = (change: Partial<ListQuery>) => router.replace(at({ ...q, ...change }));
+
+  onMount(() => void lag.ensure());
+
+  function toggle(id: string) {
+    selected = selected.includes(id) ? selected.filter((s) => s !== id) : [...selected, id];
+  }
+
+  function toggleAll() {
+    const ids = shown.map((r) => r.id);
+    selected = ids.every((id) => selected.includes(id)) ? [] : ids;
+  }
+
+  /** The rows' keys and clicks: a click anywhere on a row opens it; ↑/↓ move between rows. */
+  function rowsNav(table: HTMLTableElement) {
+    const link = (tr: Element | null | undefined) => tr?.querySelector<HTMLAnchorElement>('a.open') ?? null;
+    const click = (e: MouseEvent) => {
+      const t = e.target as Element;
+      if (t.closest('a, button, input, label') !== null) return;
+      const a = link(t.closest('tbody tr'));
+      if (a !== null) router.go(a.pathname);
+    };
+    const keys = (e: KeyboardEvent) => {
+      if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return;
+      const tr = (document.activeElement as Element | null)?.closest('tbody tr');
+      const next =
+        tr === null || tr === undefined
+          ? table.querySelector('tbody tr')
+          : e.key === 'ArrowDown'
+            ? tr.nextElementSibling
+            : tr.previousElementSibling;
+      const a = link(next);
+      if (a === null) return;
+      e.preventDefault();
+      a.focus();
+    };
+    table.addEventListener('click', click);
+    table.addEventListener('keydown', keys);
+    return () => {
+      table.removeEventListener('click', click);
+      table.removeEventListener('keydown', keys);
+    };
+  }
+
+  function lines(v: Verb, takes: ListRow[], not: ListRow[]): string[] {
+    const out = [takes.map((r) => `${r.id} on ${r.node ?? 'no node'}`).join(' · '), ACTIONS[v].each];
+    if (v === 'restart') {
+      for (const r of takes.filter((r) => r.job !== null)) {
+        out.push(`${r.id}'s running snapshot ${r.job} carries on from its last chunk after the restart.`);
+      }
+    }
+    if (takes.length > 1) out.push('One at a time, in this order; a failure stops the rest and says which.');
+    if (not.length > 0) {
+      out.push(`Not asked, ${ACTIONS[v].label.toLowerCase()} doesn't apply: ${not.map((r) => `${r.id} (${r.desired})`).join(', ')}.`);
+    }
+    return out;
+  }
+
+  const plan = $derived.by(() => {
+    if (acting === null) return null;
+    const v = acting;
+    const takes = chosen.filter((r) => ACTIONS[v].takes(r.desired));
+    const not = chosen.filter((r) => !ACTIONS[v].takes(r.desired));
+    const label = ACTIONS[v].label;
+    return {
+      verb: v,
+      ids: takes.map((r) => r.id),
+      title: takes.length === 1 ? `${label} ${takes[0].id}?` : `${label} ${takes.length} pipelines?`,
+      lines: lines(v, takes, not),
+      confirm: takes.length === 1 ? label : `${label} ${takes.length}`,
+      recorded: recordedAs(v, session) + (takes.length > 1 ? ', on each pipeline' : ''),
+      destructive: ACTIONS[v].destructive,
+    };
+  });
+
+  async function run(note: string): Promise<string | null> {
+    if (plan === null) return null;
+    const { verb, ids } = plan;
+    const o = await oneAtATime(ids, (id) => ask(api, verb, id, note));
+    selected = selected.filter((id) => !o.done.includes(id));
+    return outcomeText(o);
+  }
+
+  const asOf = $derived(last ?? 0);
+  const headings: { column: Column | null; label: string }[] = [
+    { column: 'pipeline', label: 'Pipeline' },
+    { column: 'state', label: 'State' },
+    { column: null, label: 'Source → sink' },
+    { column: 'node', label: 'Node' },
+    { column: 'lag', label: `Lag, 6 h · now / target` },
+    { column: 'changes', label: 'Changes/min' },
+    { column: 'commit', label: 'Last commit' },
+    { column: null, label: 'Tables' },
+    { column: null, label: 'Snapshot' },
+    { column: null, label: '' },
+  ];
+</script>
+
+<h2>Pipelines</h2>
+{#if stale}
+  <p class="notice" role="status">Not current: as of {clock(asOf, true)} · {duration((now - asOf) / 1000)} old</p>
+{/if}
+{#if model.read.pipelines.error !== null}
+  <p class="notice failing" role="alert">Couldn't be read, shown as last read: {model.read.pipelines.error}</p>
+{/if}
+{#if lag.error !== null}
+  <p class="notice failing" role="alert">The lag lines couldn't be read: {lag.error}</p>
+{/if}
+
+<nav class="chips" aria-label="Show">
+  {#each SHOWS as s (s.id)}
+    <a
+      href={at({ ...q, show: s.id })}
+      aria-current={q.show === s.id ? 'true' : undefined}
+      onclick={(e) => (e.preventDefault(), set({ show: s.id }))}
+    >
+      {s.label} <b>{n[s.id]}</b>
+    </a>
+  {/each}
+</nav>
+<div class="filters">
+  <label>
+    Node
+    <select value={q.node ?? ''} onchange={(e) => set({ node: e.currentTarget.value || null })}>
+      <option value="">any</option>
+      {#each nodes as node (node)}<option value={node}>{node}</option>{/each}
+    </select>
+  </label>
+  <label>
+    Source
+    <select
+      value={q.source ?? ''}
+      onchange={(e) => set({ source: (e.currentTarget.value || null) as ListQuery['source'] })}
+    >
+      <option value="">any</option>
+      <option value="postgres">PostgreSQL</option>
+      <option value="mysql">MySQL</option>
+    </select>
+  </label>
+  <input
+    type="search"
+    aria-label="Filter by name"
+    placeholder="filter by name…"
+    value={q.text}
+    oninput={(e) => set({ text: e.currentTarget.value })}
+  />
+</div>
+
+{#if can && selected.length > 0}
+  <div class="acting" role="toolbar" aria-label="Act on the selected pipelines">
+    <b>{selected.length} selected</b> ·
+    {#each offered as v (v)}
+      <button type="button" class:destructive={ACTIONS[v].destructive} disabled={offline} onclick={() => (acting = v)}>
+        {ACTIONS[v].label}…
+      </button>
+    {/each}
+    <span class="muted">Only actions your role allows appear.</span>
+    <button type="button" class="link" onclick={() => (selected = [])}>Clear</button>
+  </div>
+{/if}
+
+<table class:stale {@attach rowsNav}>
+  <thead>
+    <tr>
+      {#if can}
+        <th>
+          <input
+            type="checkbox"
+            aria-label="Select every pipeline shown"
+            checked={shown.length > 0 && shown.every((r) => selected.includes(r.id))}
+            onchange={toggleAll}
+          />
+        </th>
+      {/if}
+      {#each headings as h (h.label)}
+        <th aria-sort={h.column !== null && q.sort === h.column ? (q.desc ? 'descending' : 'ascending') : undefined}>
+          {#if h.column !== null}
+            {@const column = h.column}
+            <a href={at(sortBy(q, column))} onclick={(e) => (e.preventDefault(), set(sortBy(q, column)))}>
+              {h.label}
+            </a>
+          {:else}{h.label}{/if}
+        </th>
+      {/each}
+    </tr>
+  </thead>
+  <tbody>
+    {#each shown as r (r.id)}
+      <tr class:selected={selected.includes(r.id)}>
+        {#if can}
+          <td><input type="checkbox" aria-label="Select {r.id}" checked={selected.includes(r.id)} onchange={() => toggle(r.id)} /></td>
+        {/if}
+        <td><a class="open" href={href(r.id)}>{r.id}</a></td>
+        <td><span class="chip {r.state.tone}">{r.state.label}</span></td>
+        <td class="muted">{r.route}</td>
+        <td>{r.node ?? '—'}{#if r.away}{' '}<span class="away" title="held away from its ring owner">away</span>{/if}</td>
+        <td class="lag">
+          <Sparkline points={r.points} target={LAG_TARGET_S} />
+          {r.lag === null ? '—' : `${formatValue('seconds', r.lag)} / ${LAG_TARGET_S} s`}
+        </td>
+        <td>{r.changes === null ? '—' : r.changes}</td>
+        <td>{r.lastCommit === null ? '—' : ago(r.lastCommit, now)}</td>
+        <td>{r.halted > 0 ? `${r.tables - r.halted} of ${r.tables}` : r.tables}</td>
+        <td>{r.snapshot ?? '—'}</td>
+        <td class="note muted">{r.state.note ?? ''}</td>
+      </tr>
+    {:else}
+      <tr><td colspan="11" class="muted">No pipeline shown: change the filters.</td></tr>
+    {/each}
+  </tbody>
+</table>
+<p class="muted foot">
+  Order: red, then orange, then yellow, then by lag against the target. Click a column to sort by
+  it. The filters and sort stay in the address, so a link shows the same view. "away" = held away
+  from its ring owner.
+</p>
+
+{#if plan !== null}
+  <Dialog
+    title={plan.title}
+    lines={plan.lines}
+    recorded={plan.recorded}
+    confirm={plan.confirm}
+    destructive={plan.destructive}
+    blocked={offline
+      ? 'Offline: actions wait until the console is live again.'
+      : plan.ids.length === 0
+        ? `None of the selected pipelines takes ${ACTIONS[plan.verb].label.toLowerCase()}.`
+        : null}
+    onConfirm={run}
+    onClose={() => (acting = null)}
+  />
+{/if}
+
+<style>
+  .chips {
+    display: flex;
+    flex-wrap: wrap;
+    gap: 6px;
+    margin-bottom: 8px;
+  }
+  .chips a {
+    padding: 2px 10px;
+    border: 1px solid var(--border);
+    border-radius: 12px;
+    color: var(--text-2);
+    text-decoration: none;
+  }
+  .chips a[aria-current='true'] {
+    color: var(--text);
+    border-color: var(--accent);
+    font-weight: 600;
+  }
+  .filters {
+    display: flex;
+    gap: 12px;
+    align-items: center;
+    margin-bottom: 8px;
+    color: var(--text-2);
+  }
+  .acting {
+    display: flex;
+    gap: 6px;
+    align-items: center;
+    padding: 6px 10px;
+    margin-bottom: 8px;
+    background: var(--bg-panel);
+    border: 1px solid var(--accent);
+    border-radius: 8px;
+  }
+  .destructive {
+    color: var(--red);
+  }
+  .link {
+    border: none;
+    background: none;
+    color: var(--accent);
+  }
+  table {
+    width: 100%;
+    border-collapse: collapse;
+    background: var(--bg-panel);
+  }
+  th,
+  td {
+    text-align: left;
+    padding: 6px 8px;
+    border-bottom: 1px solid var(--border);
+    white-space: nowrap;
+  }
+  th a {
+    color: var(--text-2);
+    text-decoration: none;
+  }
+  tbody tr {
+    cursor: pointer;
+  }
+  tbody tr:hover,
+  tr.selected {
+    background: var(--bg);
+  }
+  .away {
+    color: var(--orange);
+  }
+  .note {
+    white-space: normal;
+  }
+  .stale {
+    opacity: 0.55;
+  }
+  .chip {
+    padding: 1px 8px;
+    border-radius: 10px;
+    background: var(--grey-bg);
+  }
+  .chip.green {
+    background: var(--green-bg);
+  }
+  .chip.orange {
+    background: var(--orange-bg);
+  }
+  .chip.red {
+    background: var(--red-bg);
+  }
+  .notice {
+    padding: 6px 10px;
+    border-radius: 6px;
+    background: var(--grey-bg);
+  }
+  .notice.failing {
+    background: var(--orange-bg);
+  }
+  .foot {
+    margin-top: 8px;
+  }
+</style>
````

`ui/src/pipelines/lag.svelte.ts`:

````diff
diff --git a/ui/src/pipelines/lag.svelte.ts b/ui/src/pipelines/lag.svelte.ts
new file mode 100644
index 0000000000000000000000000000000000000000..6a8e0455db4316358a968d9384536b4f84af23cc
--- /dev/null
+++ b/ui/src/pipelines/lag.svelte.ts
@@ -0,0 +1,73 @@
+// Pipelines' lag over the last 6 hours (mockups 05, 06): read once from `/v1/history`, then each
+// sample the stream sends joins its line. A read that fails keeps the lines and says why.
+
+import type { Client } from '../lib/api';
+import type { Live } from '../lib/live.svelte';
+import type { SamplesMsg } from '../model/model.svelte';
+import type { Series } from '../model/types';
+
+/** The span the lines cover. */
+export const SPAN_S = 6 * 3600;
+/** Seconds between the points read: 360 a line. */
+export const STEP_S = 60;
+
+export class LagHistory {
+  /** Each pipeline's line, `[unix seconds, seconds]`, oldest first. */
+  lines = $state<Record<string, [number, number][]>>({});
+  /** Why the last read failed, `null` when it didn't. */
+  error = $state<string | null>(null);
+
+  #api: Client;
+  #pipeline: string | null;
+  #now: () => number;
+  /** Read once: what the stream missed is read again; never read, nothing is. */
+  #asked = false;
+
+  /** Every pipeline's lines, or `pipeline`'s alone. */
+  constructor(api: Client, pipeline: string | null = null, now: () => number = Date.now) {
+    this.#api = api;
+    this.#pipeline = pipeline;
+    this.#now = now;
+  }
+
+  /** Reads the lines unless they were read already. */
+  async ensure(): Promise<void> {
+    if (!this.#asked) await this.load();
+  }
+
+  async load(): Promise<void> {
+    this.#asked = true;
+    const since = new Date(this.#now() - SPAN_S * 1000).toISOString();
+    const only = this.#pipeline === null ? '' : `&pipeline=${encodeURIComponent(this.#pipeline)}`;
+    try {
+      const { series } = await this.#api.get<{ series: Series[] }>(
+        `/v1/history?series=lag_seconds${only}&since=${since}&step=${STEP_S}`,
+      );
+      this.lines = Object.fromEntries(
+        series.filter((s) => s.pipeline !== undefined).map((s) => [s.pipeline!, s.points]),
+      );
+      this.error = null;
+    } catch (e) {
+      this.error = e instanceof Error ? e.message : String(e);
+    }
+  }
+
+  /** Follows `live`'s samples, and reads again after a gap; returns the way to stop. */
+  follow(live: Live): () => void {
+    const stops = [
+      live.onResync(() => {
+        if (this.#asked) void this.load();
+      }),
+      live.on('samples', (d) => this.sample(d as SamplesMsg)),
+    ];
+    return () => stops.forEach((s) => s());
+  }
+
+  sample(m: SamplesMsg): void {
+    const lag = m.series?.lag_seconds;
+    if (m.scope !== 'pipeline' || m.pipeline === undefined || lag === undefined) return;
+    if (this.#pipeline !== null && m.pipeline !== this.#pipeline) return;
+    const line = [...(this.lines[m.pipeline] ?? []), [m.at, lag] as [number, number]];
+    this.lines[m.pipeline] = line.filter(([t]) => t > m.at - SPAN_S);
+  }
+}
````

`ui/src/pipelines/rows.ts`:

````diff
diff --git a/ui/src/pipelines/rows.ts b/ui/src/pipelines/rows.ts
new file mode 100644
index 0000000000000000000000000000000000000000..f63eafbe5909edf92a65b8ca9eb0adb481911a2d
--- /dev/null
+++ b/ui/src/pipelines/rows.ts
@@ -0,0 +1,204 @@
+// The pipeline list (mockup 05): every pipeline, worst first, with what a person scans for; its
+// filters and sort live in the address, so a link shows the same view and Back keeps it.
+
+import { ago, duration } from '../lib/format';
+import { attention, shortJob } from '../overview/attention';
+import { rows, type Row } from '../overview/derive';
+import { at, route, tablesOf, type Facts } from '../model/facts';
+import type { Actual, Desired, Job, SourceAt } from '../model/types';
+
+export type Show = 'all' | 'attention' | 'running' | 'halted' | 'paused' | 'stopped' | 'snapshot';
+export const SHOWS: { id: Show; label: string }[] = [
+  { id: 'all', label: 'All' },
+  { id: 'attention', label: '⚠ Needs attention' },
+  { id: 'running', label: 'Running' },
+  { id: 'halted', label: 'Halted tables' },
+  { id: 'paused', label: 'Paused' },
+  { id: 'stopped', label: 'Stopped' },
+  { id: 'snapshot', label: 'Snapshotting' },
+];
+
+export type Column = 'pipeline' | 'state' | 'node' | 'lag' | 'changes' | 'commit';
+const COLUMNS: Column[] = ['pipeline', 'state', 'node', 'lag', 'changes', 'commit'];
+/** The columns sorted biggest first on their first click. */
+const NUMERIC: Column[] = ['lag', 'changes', 'commit'];
+
+/** The list's view, as its address keeps it. */
+export interface ListQuery {
+  show: Show;
+  node: string | null;
+  source: SourceAt['type'] | null;
+  text: string;
+  /** `null`: worst first. */
+  sort: Column | null;
+  desc: boolean;
+}
+
+export function parseQuery(q: URLSearchParams): ListQuery {
+  const show = q.get('show');
+  const sort = q.get('sort');
+  const source = q.get('source');
+  return {
+    show: SHOWS.some((s) => s.id === show) ? (show as Show) : 'all',
+    node: q.get('node') || null,
+    source: source === 'postgres' || source === 'mysql' ? source : null,
+    text: q.get('q') ?? '',
+    sort: COLUMNS.includes(sort as Column) ? (sort as Column) : null,
+    desc: q.has('desc'),
+  };
+}
+
+/** The query of `l`, `''` for the default view. */
+export function queryString(l: ListQuery): string {
+  const q = new URLSearchParams();
+  if (l.show !== 'all') q.set('show', l.show);
+  if (l.node !== null) q.set('node', l.node);
+  if (l.source !== null) q.set('source', l.source);
+  if (l.text !== '') q.set('q', l.text);
+  if (l.sort !== null) q.set('sort', l.sort);
+  if (l.sort !== null && l.desc) q.set('desc', '');
+  const s = q.toString().replace(/desc=(&|$)/, 'desc$1');
+  return s === '' ? '' : `?${s}`;
+}
+
+/** The view after a click on `column`'s heading: sorted by it, the other way on a second click. */
+export function sortBy(l: ListQuery, column: Column): ListQuery {
+  if (l.sort === column) return { ...l, desc: !l.desc };
+  return { ...l, sort: column, desc: NUMERIC.includes(column) };
+}
+
+export interface ListRow extends Row {
+  /** Its place worst first. */
+  order: number;
+  name: string;
+  route: string;
+  source: SourceAt['type'];
+  desired: Desired;
+  actual: Actual | null;
+  /** Held by a node other than its ring owner. */
+  away: boolean;
+  /** The changes its tables committed in the last minute; `null` without live figures. */
+  changes: number | null;
+  tables: number;
+  halted: number;
+  /** A running job's progress, else its newest ended job. */
+  snapshot: string | null;
+  /** Its running job's short id. */
+  job: string | null;
+}
+
+/** The pipeline a "Needs attention" line is about, from where it links. */
+export function pipelineOf(href: string): string | null {
+  const m = /^\/ui\/(pipelines|tables|snapshots)\/([^/~?]+)/.exec(href);
+  return m === null ? null : decodeURIComponent(m[2]);
+}
+
+function snapshotOf(jobs: Job[], now: number): { text: string | null; job: string | null } {
+  const open = jobs.find((j) => j.status === 'running' || j.status === 'queued' || j.status === 'cancelling');
+  if (open !== undefined) {
+    if (open.status !== 'running') return { text: open.status, job: shortJob(open.job_id) };
+    const pct = open.estimate ? `${Math.min(99, Math.floor((open.rows_done / open.estimate) * 100))}%` : 'running';
+    const eta = open.eta_seconds === null ? '' : ` · ETA ${duration(open.eta_seconds)}`;
+    return { text: `${pct}${eta}`, job: shortJob(open.job_id) };
+  }
+  const ended = jobs
+    .filter((j) => j.finished_at !== null)
+    .sort((a, b) => (at(b.finished_at) ?? 0) - (at(a.finished_at) ?? 0))[0];
+  if (ended === undefined) return { text: null, job: null };
+  const mark = ended.status === 'completed' ? '✓' : ended.status === 'failed' ? '✕ failed' : ended.status;
+  return { text: `${shortJob(ended.job_id)} ${mark} ${ago(at(ended.finished_at)!, now)}`, job: null };
+}
+
+/** Every pipeline's row, worst first, its lag line the last 6 hours' (`lines`). */
+export function listRows(f: Facts, lines: Record<string, [number, number][]>, now: number): ListRow[] {
+  const tables = tablesOf(f);
+  return rows(f, now).map((r, order) => {
+    const p = f.pipelines.find((x) => x.id === r.id)!;
+    const lease = f.cluster?.pipelines.find((l) => l.id === r.id);
+    const mine = tables.filter((t) => t.pipeline === r.id);
+    const fig = f.figures[r.id];
+    const jobs = f.jobs.filter((j) => j.pipeline === r.id);
+    const snap = snapshotOf(jobs, now);
+    return {
+      ...r,
+      points: lines[r.id] ?? r.points,
+      order,
+      name: p.name,
+      route: route(p),
+      source: p.source.type,
+      desired: p.desired_state,
+      actual: fig?.actual ?? p.actual_state,
+      away: r.node !== null && lease?.ring_owner != null && lease.ring_owner !== r.node,
+      changes: fig === undefined ? null : fig.tables.reduce((n, t) => n + t.changes_per_minute, 0),
+      tables: mine.length,
+      halted: mine.filter((t) => t.halt !== null).length,
+      snapshot: snap.text,
+      job: snap.job,
+    };
+  });
+}
+
+/** The pipelines "Needs attention" names. */
+export function needingAttention(f: Facts, now: number): Set<string> {
+  return new Set(
+    attention(f, now)
+      .map((a) => pipelineOf(a.href))
+      .filter((id): id is string => id !== null),
+  );
+}
+
+function shows(r: ListRow, show: Show, flagged: Set<string>): boolean {
+  switch (show) {
+    case 'all':
+      return true;
+    case 'attention':
+      return flagged.has(r.id);
+    case 'running':
+      return r.actual === 'running';
+    case 'halted':
+      return r.halted > 0;
+    case 'paused':
+      return r.actual === 'paused';
+    case 'stopped':
+      return r.actual === 'stopped';
+    case 'snapshot':
+      return r.job !== null;
+  }
+}
+
+/** How many rows each filter shows. */
+export function counts(all: ListRow[], flagged: Set<string>): Record<Show, number> {
+  return Object.fromEntries(SHOWS.map((s) => [s.id, all.filter((r) => shows(r, s.id, flagged)).length])) as Record<
+    Show,
+    number
+  >;
+}
+
+const KEY: Record<Column, (r: ListRow) => number | string> = {
+  pipeline: (r) => r.id,
+  state: (r) => r.order,
+  node: (r) => r.node ?? '',
+  lag: (r) => r.lag ?? -1,
+  changes: (r) => r.changes ?? -1,
+  commit: (r) => r.lastCommit ?? 0,
+};
+
+/** The rows `l` shows, in its order: worst first unless sorted by a column. */
+export function view(all: ListRow[], l: ListQuery, flagged: Set<string>): ListRow[] {
+  const text = l.text.trim().toLowerCase();
+  const out = all.filter(
+    (r) =>
+      shows(r, l.show, flagged) &&
+      (l.node === null || r.node === l.node) &&
+      (l.source === null || r.source === l.source) &&
+      (text === '' || r.id.toLowerCase().includes(text) || r.name.toLowerCase().includes(text)),
+  );
+  if (l.sort === null) return out;
+  const key = KEY[l.sort];
+  const sign = l.desc ? -1 : 1;
+  return out.sort((a, b) => {
+    const [x, y] = [key(a), key(b)];
+    const c = typeof x === 'number' && typeof y === 'number' ? x - y : String(x).localeCompare(String(y));
+    return sign * c || a.order - b.order;
+  });
+}
````

- [ ] **Step 4: Run the tests and the gates**

Run the step's tests again, then

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
git add -A ui/src/App.svelte ui/src/lib/router.svelte.ts ui/src/lib/router.test.ts ui/src/model/fixtures.ts ui/src/model/types.ts ui/src/pipelines/List.svelte ui/src/pipelines/List.test.ts ui/src/pipelines/lag.svelte.ts ui/src/pipelines/lag.test.ts ui/src/pipelines/rows.test.ts ui/src/pipelines/rows.ts
git commit -m "feat(ui): the pipeline list — worst first, filters and sort in the address, keys, acting on a selection"
```

---

### Task 5: A pipeline's page (`ui/src/pipelines/{detail.ts,page.svelte.ts,Pipeline.svelte}`)

**Files:**
- Modify: `ui/src/App.svelte`
- Modify: `ui/src/model/types.ts`
- Create: `ui/src/pipelines/Pipeline.svelte`
- Create: `ui/src/pipelines/Pipeline.test.ts`
- Create: `ui/src/pipelines/detail.test.ts`
- Create: `ui/src/pipelines/detail.ts`
- Create: `ui/src/pipelines/fixtures.ts`
- Create: `ui/src/pipelines/page.svelte.ts`
- Create: `ui/src/pipelines/page.test.ts`

**Interfaces:**
- Consumes: Tasks 1–4 (`Model`, `route`, `Chart`, the actions and `Dialog`, `LagHistory`,
  `SPAN_S`); M15's `shortJob`, `walPast`, `LAG_TARGET_S`, `Tone`, `REREAD_AFTER_MS`.
- Produces (`model/types.ts`): `JobDetail extends Job` (`tables`, `rate`, `requested_by`,
  `requested_role`, `note`, `truncate`, `cancel`); `PipelineDetail` (`GET /v1/pipelines/{id}`);
  `SourceDetail`'s `head`, `publication`, `binlog_expire_seconds` and the slot's `active` and
  `confirmed_flush`; `Figures.source`'s `read_at` and `behind_seconds`; `EventItem.object`.
- Produces (`ui/src/pipelines/detail.ts`, D-M16a.7): `ACTIVITY_READ` (50), `ACTIVITY_SHOWN` (10),
  `SCHEMA_SHOWN_S` (1 day); `position(tagged)`; `header(d, item, fig, lease, now): Header | null`;
  `tableLines(id, fig, d, events, now): TableLine[]`; `sourceLines(fig): SourceLine[]`;
  `snapshotPanel(jobs, now): Snapshot`; `markOf(e)`, `marks(events, since): Mark[]`;
  `activity(events, now): ActivityLine[]`.
- Produces (`ui/src/pipelines/page.svelte.ts`): `type PagePart = 'detail' | 'live' | 'jobs' |
  'events'`, `PAGE_PARTS`; `class PipelinePage(api, id, seed: Figures | null = null, now =
  Date.now)` with `id`, `detail`, `figures`, `jobs`, `events`, `read`, `missing`, `lag`, `load()`,
  `follow(live)`, `figuresMsg(m)`, `pipelineMsg(m)`, `event(e)`.
- Produces (`ui/src/pipelines/Pipeline.svelte`): props `page`, `model`, `session`, `api`, `now`,
  `stale`, `last`, `offline`.

- [ ] **Step 1: Write the failing tests**

The types gain what the page shows; `pipelines/fixtures.ts` is mockup 06's orders-pg. The tests:
`a pipeline's page` (`detail.test.ts`: the header; the list row's alone, and no node holding it;
the tables, halted first; the halts storage keeps; PostgreSQL's and MySQL's source; the snapshot;
the chart's events; the activity); `a pipeline's page data` (`page.test.ts`: drawn at once, then
read; missing, and a part that fails; the newest owner's figures; events once, and a burst read
again once; a state change); `a pipeline's page` (`Pipeline.test.ts`: what it shows; the actions
an operator's state takes and a viewer's none; a restart's dialog and note; a snapshot of the
tables chosen, truncated; an event highlighted on the chart from the activity; actions offline;
missing).

`ui/src/model/types.ts`:

````diff
diff --git a/ui/src/model/types.ts b/ui/src/model/types.ts
index 01af0c8afc08dd2353574bf8815a04a571337d45..eb2a07490af20feca7399ee950a3e8bc3f3ac283 100644
--- a/ui/src/model/types.ts
+++ b/ui/src/model/types.ts
@@ -111,11 +111,33 @@ export interface Job {
   error: string | null;
 }
 
+/** A snapshot job as a pipeline's page shows it: who asked, why, and each table's progress. */
+export interface JobDetail extends Job {
+  tables: { table: string; status: string; rows_done: number; estimate: number | null }[];
+  /** Rows a second, observed. */
+  rate: number | null;
+  requested_by: string;
+  requested_role: string | null;
+  note: string | null;
+  truncate: boolean;
+  cancel: { by: string; role: string | null; at: string; note: string | null } | null;
+}
+
 /** PostgreSQL's source detail; MySQL's has no slot. */
 export interface SourceDetail {
   engine: 'postgres' | 'mysql';
-  slot?: { name: string; wal_held_bytes: number | null } | null;
+  /** The server's write position, tagged: `pg:3/A41C2B10`. */
+  head?: string;
+  slot?: {
+    name: string;
+    wal_held_bytes: number | null;
+    active?: boolean;
+    confirmed_flush?: string | null;
+  } | null;
   max_slot_wal_keep_size?: number | null;
+  publication?: { name: string; tables: number } | null;
+  /** MySQL: how long the server keeps a binlog; `null` when it never expires them. */
+  binlog_expire_seconds?: number | null;
 }
 
 /** A pipeline's live figures: `GET /v1/pipelines/{id}/live`, and the stream's `figures`. */
@@ -127,7 +149,12 @@ export interface Figures {
   as_of: string;
   error: string | null;
   /** `null` without a worker: the pipeline is stopped, or starting. */
-  source: { detail: SourceDetail | null } | null;
+  source: {
+    detail: SourceDetail | null;
+    read_at?: string | null;
+    /** MySQL: how far the stream reads behind the source's newest change. */
+    behind_seconds?: number | null;
+  } | null;
   tables: Omit<TableItem, 'pipeline'>[];
   worker: { lag_seconds: number | null; degraded: string | null } | null;
 }
@@ -139,6 +166,8 @@ export interface EventItem {
   kind: string;
   node: string;
   pipeline?: string;
+  /** What else it concerns, by name: a table, a job id, a node. */
+  object?: string;
   severity: 'info' | 'warning' | 'error';
   text: string;
   actor?: { name: string; role: string };
@@ -153,3 +182,26 @@ export interface Series {
   pipeline?: string;
   points: [number, number][];
 }
+
+/** `GET /v1/pipelines/{id}`: the pipeline, its lease and its halted tables, from storage. */
+export interface PipelineDetail {
+  id: string;
+  name: string;
+  desired_state: Desired;
+  actual_state: Actual | null;
+  source: SourceAt;
+  sink: SinkAt;
+  error: string | null;
+  degraded: string | null;
+  holder: {
+    node: string;
+    owner_epoch: number;
+    renewed_at: string;
+    released: boolean;
+    stale: boolean;
+    suspect: boolean;
+  } | null;
+  /** The committed position, tagged. */
+  position: string | null;
+  halted_tables: { table: string; code: string; detail: string; since: string | null }[];
+}
````

`ui/src/pipelines/Pipeline.test.ts`:

````diff
diff --git a/ui/src/pipelines/Pipeline.test.ts b/ui/src/pipelines/Pipeline.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..6119dd0da466a86ca7654af93ae963958eb34652
--- /dev/null
+++ b/ui/src/pipelines/Pipeline.test.ts
@@ -0,0 +1,185 @@
+import { flushSync, mount, tick, unmount } from 'svelte';
+import { afterEach, describe, expect, it, vi } from 'vitest';
+import type { Client } from '../lib/api';
+import type { Role, Session } from '../lib/session';
+import { facts, NOW } from '../model/fixtures';
+import { Model } from '../model/model.svelte';
+import { detail, events, jobs, liveFigures } from './fixtures';
+import { PipelinePage } from './page.svelte';
+import Pipeline from './Pipeline.svelte';
+
+// The chart's uPlot: a second a pixel, drawing its hooks once.
+vi.mock('uplot', () => ({
+  default: class {
+    bbox = { left: 0, top: 0, width: 600, height: 180 };
+    ctx = { save() {}, restore() {}, beginPath() {}, moveTo() {}, lineTo() {}, stroke() {}, setLineDash() {}, lineWidth: 0, strokeStyle: '' };
+    cursor = { idx: null, left: -10 };
+    #opts: { hooks: { draw: ((u: unknown) => void)[] } };
+    constructor(opts: never) {
+      this.#opts = opts;
+      this.redraw();
+    }
+    valToPos(v: number) {
+      return v % 600;
+    }
+    setData() {}
+    setScale() {}
+    setSize() {}
+    redraw() {
+      for (const h of this.#opts.hooks.draw) h(this);
+    }
+    destroy() {}
+  },
+}));
+
+let mounted: ReturnType<typeof mount> | undefined;
+afterEach(() => {
+  if (mounted !== undefined) unmount(mounted);
+  mounted = undefined;
+});
+
+function client(): Client & { sent: unknown[][] } {
+  const sent: unknown[][] = [];
+  return {
+    sent,
+    get: async <T,>() => ({}) as T,
+    send: async <T,>(method: string, path: string, body?: unknown) => {
+      sent.push([method, path, body]);
+      return {} as T;
+    },
+  };
+}
+
+const session = (role: Role): Session => ({ name: 'Oleksii', role, auth: 'basic', expires: null, signed_in: null, csrf: 't' });
+
+async function render(over: { role?: Role; offline?: boolean; missing?: boolean; api?: Client } = {}) {
+  const api = over.api ?? client();
+  const page = new PipelinePage(api, 'orders-pg', liveFigures(), () => NOW);
+  page.detail = detail();
+  page.jobs = jobs();
+  page.events = events();
+  page.missing = over.missing ?? false;
+  const model = new Model(api);
+  model.facts = facts();
+  const target = document.createElement('div');
+  document.body.replaceChildren(target);
+  mounted = mount(Pipeline, {
+    target,
+    props: { page, model, session: session(over.role ?? 'operator'), api, now: NOW, stale: false, last: NOW, offline: over.offline ?? false },
+  });
+  await vi.dynamicImportSettled();
+  await tick();
+  flushSync();
+  return target;
+}
+
+const settle = async () => {
+  for (let i = 0; i < 5; i++) await tick();
+  flushSync();
+};
+
+function button(t: HTMLElement, text: string): HTMLButtonElement {
+  return [...t.querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent?.trim() === text)!;
+}
+
+describe("a pipeline's page", () => {
+  it('shows its header, tables, source, snapshot and activity', async () => {
+    const t = await render();
+    expect(t.querySelector('h2')?.textContent).toContain('orders-pg');
+    expect(t.querySelector('h2 .chip')?.textContent).toBe('1 table halted');
+    expect(t.querySelector('.where')?.textContent).toBe(
+      'PostgreSQL orders@pg-prod-replica → Delta lake/orders-pg · on n2 · away from owner n1 · lease epoch 2, expires in 8 s',
+    );
+    expect([...t.querySelectorAll('tbody tr td:first-child a')].map((a) => a.textContent)).toEqual(['public.refunds', 'public.orders']);
+    expect(t.textContent).toContain('da_orders · 14 tables');
+    expect(t.textContent).toContain('61% · 2.6 M of ~4.2 M rows · ETA 7m 0s · 41 k rows/s');
+    expect(t.querySelectorAll('.activity li')).toHaveLength(7);
+  });
+
+  it('offers an operator the actions its state takes, and a viewer none', async () => {
+    let t = await render();
+    expect([...t.querySelectorAll('.actions button')].map((b) => b.textContent?.trim())).toEqual([
+      'Flush',
+      'Pause',
+      'Restart',
+      'Stop',
+      'Snapshot…',
+    ]);
+    unmount(mounted!);
+    t = await render({ role: 'viewer' });
+    expect(t.querySelector('.actions')).toBeNull();
+  });
+
+  it('says what a restart does to it, and sends the note', async () => {
+    const api = client();
+    const t = await render({ api });
+    button(t, 'Restart').click();
+    flushSync();
+    const dialog = t.querySelector('[role="dialog"]')!;
+    expect(dialog.textContent).toContain('orders-pg stops on n2,');
+    expect(dialog.textContent).toContain('Its running snapshot 0192f0c4 carries on from its last chunk.');
+    expect(dialog.textContent).toContain('Recorded as: restarted by Oleksii (operator)');
+    const area = dialog.querySelector('textarea')!;
+    area.value = 'apply sink.prefix';
+    area.dispatchEvent(new Event('input'));
+    dialog.querySelector<HTMLButtonElement>('.go')!.click();
+    await settle();
+    expect(api.sent).toEqual([['POST', '/v1/pipelines/orders-pg/restart', { note: 'apply sink.prefix' }]]);
+    expect(t.querySelector('[role="dialog"]')).toBeNull();
+  });
+
+  it('starts a snapshot job of the tables chosen, truncating them when asked', async () => {
+    const api = client();
+    const t = await render({ api });
+    button(t, 'Snapshot…').click();
+    flushSync();
+    const dialog = t.querySelector('[role="dialog"]')!;
+    expect(dialog.querySelector<HTMLButtonElement>('.go')!.disabled).toBe(true);
+    expect(dialog.textContent).toContain('Choose the tables to copy.');
+    const boxes = dialog.querySelectorAll<HTMLInputElement>('input[type="checkbox"]');
+    boxes[0].click();
+    boxes[boxes.length - 1].click();
+    flushSync();
+    expect(dialog.textContent).toContain('Truncate empties each table first');
+    dialog.querySelector<HTMLButtonElement>('.go')!.click();
+    await settle();
+    expect(api.sent).toEqual([['POST', '/v1/pipelines/orders-pg/snapshots', { tables: ['public.refunds'], truncate: true }]]);
+  });
+
+  it("highlights an event on the chart while its activity line is hovered", async () => {
+    const t = await render();
+    const takeover = t.querySelectorAll('.activity li')[0];
+    takeover.dispatchEvent(new MouseEvent('mouseenter'));
+    flushSync();
+    expect(t.querySelector('.label.on')?.textContent).toBe('failover');
+    expect(takeover.classList.contains('on')).toBe(true);
+  });
+
+  it('keeps its actions from being taken while offline', async () => {
+    const t = await render({ offline: true });
+    expect(button(t, 'Restart').disabled).toBe(true);
+  });
+
+  it("says when its source's detail isn't read yet, apart from when no worker runs it", async () => {
+    const api = client();
+    const page = new PipelinePage(api, 'orders-pg', { ...liveFigures(), source: { detail: null, read_at: null } }, () => NOW);
+    const model = new Model(api);
+    model.facts = facts();
+    const target = document.createElement('div');
+    document.body.replaceChildren(target);
+    mounted = mount(Pipeline, {
+      target,
+      props: { page, model, session: session('operator'), api, now: NOW, stale: false, last: NOW, offline: false },
+    });
+    flushSync();
+    expect(target.querySelector('aside')?.textContent).toContain("Not read yet: the node that runs it reads the source's detail at each heartbeat");
+    page.figures = { ...liveFigures(), source: null, worker: null };
+    flushSync();
+    expect(target.querySelector('aside')?.textContent).toContain('No worker runs it now');
+  });
+
+  it('says when the generation has no such pipeline', async () => {
+    const t = await render({ missing: true });
+    expect(t.querySelector('[role="alert"]')?.textContent).toBe('There is no pipeline orders-pg in the generation this node runs.');
+  });
+});
````

`ui/src/pipelines/detail.test.ts`:

````diff
diff --git a/ui/src/pipelines/detail.test.ts b/ui/src/pipelines/detail.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..f00d9ae73ece9a2f24b4e9919bdff160f86975f7
--- /dev/null
+++ b/ui/src/pipelines/detail.test.ts
@@ -0,0 +1,95 @@
+import { describe, expect, it } from 'vitest';
+import { facts, NOW } from '../model/fixtures';
+import { activity, header, marks, position, snapshotPanel, sourceLines, tableLines } from './detail';
+import { detail, events, jobs, liveFigures } from './fixtures';
+
+const lease = () => facts().cluster!.pipelines[0];
+
+describe("a pipeline's page", () => {
+  it("heads with its state, its route written out, its node, away from its owner, and its lease", () => {
+    expect(header(detail(), undefined, liveFigures(), lease(), NOW)).toEqual({
+      state: { label: '1 table halted', tone: 'red' },
+      route: 'PostgreSQL orders@pg-prod-replica → Delta lake/orders-pg',
+      where: 'on n2 · away from owner n1 · lease epoch 2, expires in 8 s',
+    });
+  });
+
+  it("heads with its list row's alone until its own read, and says when no node holds it", () => {
+    const item = { ...facts().pipelines[3], holder: null };
+    expect(header(null, item, null, undefined, NOW)).toEqual({
+      state: { label: 'running', tone: 'green' },
+      route: 'PostgreSQL crm@pg-crm → Delta lake/customers',
+      where: 'no node holds its lease',
+    });
+    expect(header(null, undefined, null, undefined, NOW)).toBeNull();
+  });
+
+  it('lists its tables halted first, with figures, cost, and their last schema change', () => {
+    const lines = tableLines('orders-pg', liveFigures(), detail(), events(), NOW);
+    expect(lines.map((t) => [t.table, t.state.label, t.changes, t.version, t.files, t.schema])).toEqual([
+      ['public.refunds', 'halted', 0, 2210, null, "amount changed int → text, which can't be applied in place"],
+      ['public.orders', 'running', 1840, 18044, '6 of 820', 'public.orders: column reason added'],
+    ]);
+    expect(lines[0].href).toBe('/ui/tables/orders-pg/public.refunds');
+  });
+
+  it('lists the halts storage keeps while no worker shows its tables', () => {
+    const lines = tableLines('orders-pg', null, detail(), [], NOW);
+    expect(lines.map((t) => [t.table, t.state.label, t.changes, t.schema])).toEqual([
+      ['public.refunds', 'halted', null, "amount changed int → text, which can't be applied in place"],
+    ]);
+  });
+
+  it("shows PostgreSQL's slot, WAL held against its limit, the confirmed LSN and the publication", () => {
+    expect(sourceLines(liveFigures()).map((l) => [l.label, l.value, l.tone])).toEqual([
+      ['Slot', 'da_orders active', 'normal'],
+      ['WAL held', '210 MB of max_slot_wal_keep_size 3 GB', 'normal'],
+      ['Confirmed LSN', '3/A41C2B10', 'normal'],
+      ['Publication', 'da_orders · 14 tables', 'normal'],
+    ]);
+    const f = liveFigures();
+    f.source!.detail!.slot = { name: 'da_orders', active: false, confirmed_flush: null, wal_held_bytes: 2 * 1024 ** 3 };
+    f.source!.detail!.publication = null;
+    expect(sourceLines(f).map((l) => [l.value, l.tone])).toEqual([
+      ['da_orders inactive', 'orange'],
+      ['2 GB of max_slot_wal_keep_size 3 GB', 'orange'],
+      ['—', 'normal'],
+      ['none by its name', 'red'],
+    ]);
+  });
+
+  it("shows MySQL's position, how long it keeps binlogs, and how far behind it reads; nothing without a worker", () => {
+    const f = liveFigures();
+    f.source = { detail: { engine: 'mysql', head: 'mysql-gtid:3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77', binlog_expire_seconds: 604_800 }, behind_seconds: 3.2 };
+    expect(sourceLines(f).map((l) => [l.label, l.value])).toEqual([
+      ['Position', '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77'],
+      ['Binlogs kept', '7d 0h'],
+      ['Behind', '3 s'],
+    ]);
+    expect(sourceLines({ ...f, source: null })).toEqual([]);
+    expect(position('binlog:mysql-bin.000042:1337')).toBe('mysql-bin.000042:1337');
+  });
+
+  it('shows the running job with its progress, and the three ended last', () => {
+    const s = snapshotPanel(jobs(), NOW);
+    expect(s.current).toEqual({
+      job: '0192f0c4',
+      href: '/ui/snapshots/orders-pg~0192f0c47d5e7d5e8f0a1b2c3d4e5f60',
+      what: `public.order_items · started by Oleksii ${new Date(Date.UTC(2026, 10, 25, 13, 10)).toTimeString().slice(0, 5)}`,
+      note: 'backfill after the 12:40 outage',
+      progress: '61% · 2.6 M of ~4.2 M rows · ETA 7m 0s · 41 k rows/s',
+    });
+    expect(s.earlier.map((j) => `${j.job} ${j.text}`)).toEqual(['019aa1c2 cancelled by ci-bot 1 d ago', '019aa0a1 ✓ 2 d ago']);
+  });
+
+  it("draws the last 6 hours' events it labels on the chart: actions carried out, halts, failovers, jobs", () => {
+    const since = NOW / 1000 - 6 * 3600;
+    expect(marks(events(), since).map((m) => m.label)).toEqual(['failover', 'public.refunds halted', 'snapshot', 'restarted']);
+  });
+
+  it('lists the activity, who did each action and why', () => {
+    const lines = activity(events(), NOW);
+    expect(lines[3]).toMatchObject({ text: 'started job 0192f0c4 on orders-pg', who: 'Oleksii (operator)', note: 'backfill after the 12:40 outage' });
+    expect(lines[0].when).toMatch(/^\d\d:\d\d$/);
+  });
+});
````

`ui/src/pipelines/fixtures.ts`:

````diff
diff --git a/ui/src/pipelines/fixtures.ts b/ui/src/pipelines/fixtures.ts
new file mode 100644
index 0000000000000000000000000000000000000000..e08395dac57c9b0a60f391c2f8e11e00ef5a755b
--- /dev/null
+++ b/ui/src/pipelines/fixtures.ts
@@ -0,0 +1,105 @@
+// The tests' pipeline page: mockup 06's orders-pg, in the API's shapes.
+
+import { event, facts, iso, NOW } from '../model/fixtures';
+import type { EventItem, Figures, JobDetail, PipelineDetail } from '../model/types';
+
+const MIN = 60_000;
+
+export function detail(over: Partial<PipelineDetail> = {}): PipelineDetail {
+  const p = facts().pipelines[0];
+  return {
+    id: p.id,
+    name: p.name,
+    desired_state: 'running',
+    actual_state: 'running',
+    source: p.source,
+    sink: p.sink,
+    error: null,
+    degraded: null,
+    holder: { node: 'n2', owner_epoch: 2, renewed_at: iso(NOW - 2_000), released: false, stale: false, suspect: false },
+    position: 'pg:3/A41C2B10',
+    halted_tables: [{ table: 'public.refunds', code: 'SCHEMA_REFUSED', detail: "amount changed int → text, which can't be applied in place", since: iso(NOW - 12 * MIN) }],
+    ...over,
+  };
+}
+
+/** orders-pg's figures, with its source's detail. */
+export function liveFigures(): Figures {
+  const f = facts().figures['orders-pg'];
+  return {
+    ...f,
+    source: {
+      detail: {
+        engine: 'postgres',
+        head: 'pg:3/A41C9000',
+        slot: { name: 'da_orders', active: true, confirmed_flush: 'pg:3/A41C2B10', wal_held_bytes: 210 * 1024 ** 2 },
+        max_slot_wal_keep_size: 3 * 1024 ** 3,
+        publication: { name: 'da_orders', tables: 14 },
+      },
+      read_at: iso(NOW - 4_000),
+    },
+  };
+}
+
+export function job(over: Partial<JobDetail> & Pick<JobDetail, 'job_id' | 'status'>): JobDetail {
+  return {
+    pipeline: 'orders-pg',
+    rows_done: 0,
+    estimate: null,
+    eta_seconds: null,
+    requested_at: iso(NOW - 60 * MIN),
+    finished_at: null,
+    error: null,
+    tables: [{ table: 'public.order_items', status: 'copying', rows_done: 0, estimate: null }],
+    rate: null,
+    requested_by: 'Oleksii',
+    requested_role: 'operator',
+    note: null,
+    truncate: false,
+    cancel: null,
+    ...over,
+  };
+}
+
+export function jobs(): JobDetail[] {
+  return [
+    job({ job_id: 'orders-pg~019aa0a1b2c37d5e8f0a1b2c3d4e5f60', status: 'completed', requested_at: iso(NOW - 3 * 86_400_000), finished_at: iso(NOW - 2 * 86_400_000) }),
+    job({
+      job_id: 'orders-pg~019aa1c2d3e47d5e8f0a1b2c3d4e5f60',
+      status: 'cancelled',
+      requested_at: iso(NOW - 26 * 3_600_000),
+      finished_at: iso(NOW - 25 * 3_600_000),
+      cancel: { by: 'ci-bot', role: 'operator', at: iso(NOW - 25 * 3_600_000), note: null },
+    }),
+    job({
+      job_id: 'orders-pg~0192f0c47d5e7d5e8f0a1b2c3d4e5f60',
+      status: 'running',
+      requested_at: iso(Date.UTC(2026, 10, 25, 13, 10)),
+      rows_done: 2_600_000,
+      estimate: 4_200_000,
+      eta_seconds: 420,
+      rate: 41_000,
+      note: 'backfill after the 12:40 outage',
+    }),
+  ];
+}
+
+export function events(): EventItem[] {
+  return [
+    event({ kind: 'lease.takeover', at: iso(NOW - 7 * MIN), pipeline: 'orders-pg', text: 'orders-pg taken over by n2 from n1 · epoch 1 → 2' }),
+    event({ kind: 'table.halted', at: iso(NOW - 12 * MIN), pipeline: 'orders-pg', object: 'public.refunds', severity: 'error', text: 'public.refunds halted: SCHEMA_REFUSED' }),
+    event({ kind: 'schema.applied', at: iso(NOW - 40 * MIN), pipeline: 'orders-pg', object: 'public.orders', text: 'public.orders: column reason added' }),
+    event({
+      kind: 'action.snapshot.start',
+      at: iso(NOW - 52 * MIN),
+      pipeline: 'orders-pg',
+      text: 'started job 0192f0c4 on orders-pg',
+      actor: { name: 'Oleksii', role: 'operator' },
+      note: 'backfill after the 12:40 outage',
+      outcome: 'accepted',
+    }),
+    event({ kind: 'action.pipeline.restart', at: iso(NOW - 3 * 3_600_000), pipeline: 'orders-pg', text: 'restart orders-pg', actor: { name: 'Oleksii', role: 'operator' }, outcome: 'refused' }),
+    event({ kind: 'action.pipeline.restart', at: iso(NOW - 4 * 3_600_000), pipeline: 'orders-pg', text: 'restart orders-pg', actor: { name: 'Oleksii', role: 'operator' }, note: 'apply sink.prefix', outcome: 'accepted' }),
+    event({ kind: 'action.pipeline.pause', at: iso(NOW - 7 * 3_600_000), pipeline: 'orders-pg', text: 'pause orders-pg', outcome: 'accepted' }),
+  ];
+}
````

`ui/src/pipelines/page.test.ts`:

````diff
diff --git a/ui/src/pipelines/page.test.ts b/ui/src/pipelines/page.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..ff0235d3f210b7a9490d77ad9744faded49d5570
--- /dev/null
+++ b/ui/src/pipelines/page.test.ts
@@ -0,0 +1,96 @@
+import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
+import { HttpError, type Client } from '../lib/api';
+import { event, iso, NOW } from '../model/fixtures';
+import { REREAD_AFTER_MS } from '../model/model.svelte';
+import { detail, events, jobs, liveFigures } from './fixtures';
+import { PipelinePage } from './page.svelte';
+
+/** A client answering each path from `answers` (an error is thrown); others fail with 503. */
+function api(answers: Record<string, unknown>): Client & { asked: string[] } {
+  const asked: string[] = [];
+  return {
+    asked,
+    get: async <T,>(path: string) => {
+      asked.push(path);
+      const a = Object.entries(answers).find(([p]) => path.startsWith(p))?.[1];
+      if (a === undefined) throw new HttpError(503, `${path} is unavailable`);
+      if (a instanceof Error) throw a;
+      return a as T;
+    },
+    send: async () => {
+      throw new Error('reads only');
+    },
+  };
+}
+
+const READS = {
+  '/v1/pipelines/orders-pg/live': liveFigures(),
+  '/v1/pipelines/orders-pg/snapshots': { jobs: jobs() },
+  '/v1/pipelines/orders-pg': detail(),
+  '/v1/events?object=orders-pg&limit=50': { events: events() },
+  '/v1/history?series=lag_seconds&pipeline=orders-pg': { series: [{ name: 'lag_seconds', pipeline: 'orders-pg', points: [[NOW / 1000, 21]] }] },
+};
+
+describe("a pipeline's page data", () => {
+  beforeEach(() => vi.useFakeTimers());
+  afterEach(() => vi.useRealTimers());
+
+  it('draws the figures the stream sent at once, then reads each part', async () => {
+    const seed = { ...liveFigures(), owner_epoch: 1 };
+    const a = api(READS);
+    const p = new PipelinePage(a, 'orders-pg', seed, () => NOW);
+    expect(p.figures).toEqual(seed);
+    await p.load();
+    expect(p.detail?.position).toBe('pg:3/A41C2B10');
+    expect(p.figures?.owner_epoch).toBe(2);
+    expect(p.jobs).toHaveLength(3);
+    expect(p.events).toHaveLength(7);
+    expect(p.lag.lines['orders-pg']).toEqual([[NOW / 1000, 21]]);
+    expect(Object.values(p.read).every((r) => r.error === null)).toBe(true);
+  });
+
+  it('says a pipeline the generation lacks is missing, and keeps a part that fails to read', async () => {
+    const p = new PipelinePage(
+      api({ ...READS, '/v1/pipelines/orders-pg/live': new HttpError(409, "no node holds orders-pg's lease"), '/v1/pipelines/orders-pg': new HttpError(404, 'no such pipeline') }),
+      'orders-pg',
+      liveFigures(),
+      () => NOW,
+    );
+    await p.load();
+    expect(p.missing).toBe(true);
+    expect(p.read.live.error).toBe("no node holds orders-pg's lease");
+    expect(p.figures).not.toBeNull();
+  });
+
+  it("takes its own figures from the stream, the newest owner's, and no other pipeline's", () => {
+    const p = new PipelinePage(api({}), 'orders-pg', liveFigures(), () => NOW);
+    p.figuresMsg({ ...liveFigures(), id: 'crm-pg', owner_epoch: 9 });
+    p.figuresMsg({ ...liveFigures(), owner_epoch: 1, node: 'n1' });
+    expect(p.figures?.node).toBe('n2');
+    p.figuresMsg({ ...liveFigures(), owner_epoch: 3, node: 'n3' });
+    expect(p.figures?.node).toBe('n3');
+  });
+
+  it('adds its events once, and reads again, once, what a burst of them changes', async () => {
+    const a = api(READS);
+    const p = new PipelinePage(a, 'orders-pg', null, () => NOW);
+    const done = event({ kind: 'job.completed', at: iso(NOW), pipeline: 'orders-pg', text: 'job 0192f0c4 completed' });
+    p.event(done);
+    p.event(done);
+    p.event(event({ kind: 'action.pipeline.pause', at: iso(NOW), pipeline: 'orders-pg', text: 'pause', outcome: 'accepted' }));
+    p.event(event({ kind: 'job.failed', at: iso(NOW), pipeline: 'crm-pg', text: 'not this one' }));
+    expect(p.events.map((e) => e.kind)).toEqual(['action.pipeline.pause', 'job.completed']);
+    await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
+    expect(a.asked.sort()).toEqual(['/v1/pipelines/orders-pg', '/v1/pipelines/orders-pg/snapshots']);
+  });
+
+  it("takes a state change from the node that runs it, and reads its detail again", async () => {
+    const a = api(READS);
+    const p = new PipelinePage(a, 'orders-pg', liveFigures(), () => NOW);
+    p.pipelineMsg({ pipeline: 'orders-pg', actual: 'paused', error: null, node: 'n2', owner_epoch: 2 });
+    p.pipelineMsg({ pipeline: 'orders-pg', actual: 'standby', error: null, node: 'n1', owner_epoch: 1 });
+    expect(p.figures?.actual).toBe('paused');
+    await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
+    expect(a.asked).toEqual(['/v1/pipelines/orders-pg']);
+  });
+});
````

- [ ] **Step 2: Run them to see them fail**

Run: `(cd ui && npm test)`
Expected: `detail.test.ts`, `page.test.ts` and `Pipeline.test.ts` fail to load (`Failed to resolve
import "./detail"`, `"./page.svelte"`, `"./Pipeline.svelte"`); every other file passes.

- [ ] **Step 3: Implement**

`detail.ts`, `page.svelte.ts`, `Pipeline.svelte`; `App.svelte` makes a `PipelinePage` for the id
in `/ui/pipelines/{id}`, seeded with the figures the model has, and follows the stream while it
is shown.

`ui/src/App.svelte`:

````diff
diff --git a/ui/src/App.svelte b/ui/src/App.svelte
index ef3a09e80d87b55640caa4d1b6d56ba41db2ef96..296682b455e0a885b6e42ed04c97ad747ac8838f 100644
--- a/ui/src/App.svelte
+++ b/ui/src/App.svelte
@@ -1,6 +1,6 @@
 <script lang="ts">
   // The console's app: who is signed in, the live stream, and the page the address names.
-  import { onMount } from 'svelte';
+  import { onMount, untrack } from 'svelte';
   import Frame from './Frame.svelte';
   import Later from './Later.svelte';
   import { client } from './lib/api';
@@ -11,6 +11,8 @@
   import Overview from './overview/Overview.svelte';
   import { LagHistory } from './pipelines/lag.svelte';
   import List from './pipelines/List.svelte';
+  import { PipelinePage } from './pipelines/page.svelte';
+  import Pipeline from './pipelines/Pipeline.svelte';
 
   // The server says why the session ended: its 401 or 403 page, or the sign-in.
   const ended = () => location.reload();
@@ -30,6 +32,20 @@
   const model = new Model(api);
   /** Every pipeline's lag over 6 hours, read when the list is first shown. */
   const lag = new LagHistory(api);
+  /** The pipeline whose page is shown; its page reads and follows it while shown. */
+  const pageId = $derived(router.section === 'pipelines' && router.rest.length === 1 ? router.rest[0] : null);
+  let page = $state<PipelinePage | null>(null);
+  $effect(() => {
+    if (pageId === null || session === null) {
+      page = null;
+      return;
+    }
+    // What the stream sent already is drawn at once (Admin_Console §8: now first).
+    const p = new PipelinePage(api, pageId, untrack(() => model.facts.figures[pageId] ?? null));
+    page = p;
+    void p.load();
+    return p.follow(live);
+  });
 
   async function readGeneration() {
     try {
@@ -82,6 +98,17 @@
   >
     {#if router.section === 'overview'}
       <Overview {model} {now} stale={live.stale(now)} last={live.last} />
+    {:else if page !== null}
+      <Pipeline
+        {page}
+        {model}
+        {session}
+        {api}
+        {now}
+        stale={live.stale(now)}
+        last={live.last}
+        offline={live.state === 'offline'}
+      />
     {:else if router.section === 'pipelines' && router.rest.length === 0}
       <List
         {model}
````

`ui/src/pipelines/Pipeline.svelte`:

````diff
diff --git a/ui/src/pipelines/Pipeline.svelte b/ui/src/pipelines/Pipeline.svelte
new file mode 100644
index 0000000000000000000000000000000000000000..1ef2f1866e15d8f16922e4bef3f3d2be9ec12c6c
--- /dev/null
+++ b/ui/src/pipelines/Pipeline.svelte
@@ -0,0 +1,387 @@
+<script lang="ts">
+  // A pipeline's page (Admin_Console §4, mockup 06): the header with its source, sink, node and
+  // lease; its actions; the lag over 6 hours with its events; its tables; its source; its
+  // snapshot; its activity. Hovering an event on the chart highlights it in the activity, and the
+  // reverse.
+  import type { Client } from '../lib/api';
+  import { ACTIONS, ask, mayAct, noteBody, recordedAs, VERBS, type Target, type Verb } from '../lib/actions';
+  import Chart from '../lib/Chart.svelte';
+  import Dialog from '../lib/Dialog.svelte';
+  import { ago, clock, duration } from '../lib/format';
+  import type { Session } from '../lib/session';
+  import type { Model } from '../model/model.svelte';
+  import { LAG_TARGET_S } from '../overview/attention';
+  import { activity, header, marks, snapshotPanel, sourceLines, tableLines } from './detail';
+  import { SPAN_S } from './lag.svelte';
+  import { PAGE_PARTS, type PipelinePage } from './page.svelte';
+
+  interface Props {
+    page: PipelinePage;
+    model: Model;
+    session: Session;
+    api: Client;
+    now: number;
+    stale: boolean;
+    last: number | null;
+    offline: boolean;
+  }
+  let { page, model, session, api, now, stale, last, offline }: Props = $props();
+
+  const id = $derived(page.id);
+  const item = $derived(model.facts.pipelines.find((p) => p.id === id));
+  const lease = $derived(model.facts.cluster?.pipelines.find((l) => l.id === id));
+  const head = $derived(header(page.detail, item, page.figures, lease, now));
+  const tables = $derived(tableLines(id, page.figures, page.detail, page.events, now));
+  const source = $derived(sourceLines(page.figures));
+  const snap = $derived(snapshotPanel(page.jobs, now));
+  const lines = $derived(activity(page.events, now));
+  const since = $derived(now / 1000 - SPAN_S);
+  const drawn = $derived(marks(page.events, since));
+  const desired = $derived(page.detail?.desired_state ?? item?.desired_state ?? null);
+  const can = $derived(mayAct(session.role) && desired !== null);
+  const offered = $derived(desired === null ? [] : VERBS.filter((v) => ACTIONS[v].takes(desired)));
+  const failing = $derived(
+    PAGE_PARTS.filter((p) => page.read[p].error !== null).map((p) => `${p} (${page.read[p].error})`),
+  );
+  const asOf = $derived(last ?? 0);
+
+  let hovered = $state<string | null>(null);
+  let acting = $state<Verb | null>(null);
+  let snapping = $state(false);
+  let chosen = $state<string[]>([]);
+  let truncate = $state(false);
+
+  const target = $derived<Target | null>(
+    desired === null
+      ? null
+      : {
+          id,
+          node: page.figures?.node ?? page.detail?.holder?.node ?? item?.holder ?? null,
+          desired,
+          actual: page.figures?.actual ?? page.detail?.actual_state ?? null,
+          job: snap.current?.job ?? null,
+        },
+  );
+  const blocked = $derived(offline ? 'Offline: actions wait until the console is live again.' : null);
+
+  async function act(v: Verb, note: string): Promise<string | null> {
+    try {
+      await ask(api, v, id, note);
+      return null;
+    } catch (e) {
+      return e instanceof Error ? e.message : String(e);
+    }
+  }
+
+  async function snapshot(note: string): Promise<string | null> {
+    try {
+      await api.send('POST', `/v1/pipelines/${encodeURIComponent(id)}/snapshots`, {
+        tables: chosen,
+        truncate,
+        ...noteBody(note),
+      });
+      chosen = [];
+      truncate = false;
+      return null;
+    } catch (e) {
+      return e instanceof Error ? e.message : String(e);
+    }
+  }
+
+  const pick = (t: string) => (chosen = chosen.includes(t) ? chosen.filter((c) => c !== t) : [...chosen, t]);
+</script>
+
+{#if page.missing}
+  <h2>{id}</h2>
+  <p role="alert">There is no pipeline {id} in the generation this node runs.</p>
+  <a href="/ui/pipelines">All pipelines →</a>
+{:else}
+  <h2>
+    {id}
+    {#if head !== null}<span class="chip {head.state.tone}">{head.state.label}</span>{/if}
+  </h2>
+  {#if head !== null}<p class="where muted">{head.route} · {head.where}</p>{/if}
+  {#if page.figures?.error ?? page.detail?.error}
+    <p class="error" role="alert">{page.figures?.error ?? page.detail?.error}</p>
+  {/if}
+  {#if can && target !== null}
+    <div class="actions">
+      {#each offered as v (v)}
+        <button type="button" class:destructive={ACTIONS[v].destructive} disabled={offline} onclick={() => (acting = v)}>
+          {ACTIONS[v].label}
+        </button>
+      {/each}
+      {#if desired === 'running'}
+        <button type="button" disabled={offline} onclick={() => (snapping = true)}>Snapshot…</button>
+      {/if}
+    </div>
+  {/if}
+  {#if stale}
+    <p class="notice" role="status">Not current: as of {clock(asOf, true)} · {duration((now - asOf) / 1000)} old</p>
+  {/if}
+  {#if failing.length > 0}
+    <p class="notice failing" role="alert">Couldn't be read, shown as last read: {failing.join('; ')}</p>
+  {/if}
+
+  <section class="panel" class:stale>
+    <Chart
+      label="Lag, last 6 hours"
+      unit="seconds"
+      points={page.lag.lines[id] ?? []}
+      {since}
+      until={now / 1000}
+      target={LAG_TARGET_S}
+      marks={drawn}
+      {hovered}
+      onHover={(k) => (hovered = k)}
+    />
+  </section>
+
+  <div class="cols">
+    <section class="panel" class:stale>
+      <h3>Tables · {tables.length}</h3>
+      <table>
+        <thead>
+          <tr><th>Table</th><th>State</th><th>Changes/min</th><th>Last commit</th><th>Delta v</th><th>Files / MERGE</th><th>Schema</th></tr>
+        </thead>
+        <tbody>
+          {#each tables as t (t.table)}
+            <tr>
+              <td><a href={t.href}>{t.table}</a></td>
+              <td><span class="chip {t.state.tone}">{t.state.label}</span></td>
+              <td>{t.changes ?? '—'}</td>
+              <td>{t.lastCommit === null ? '—' : ago(t.lastCommit, now)}</td>
+              <td>{t.version ?? '—'}</td>
+              <td>{t.files ?? '—'}</td>
+              <td class="schema">{t.schema}</td>
+            </tr>
+          {:else}
+            <tr><td colspan="7" class="muted">No table known yet: no worker runs it, and none is halted.</td></tr>
+          {/each}
+        </tbody>
+      </table>
+    </section>
+
+    <aside>
+      <section class="panel" class:stale>
+        <h3>Source</h3>
+        {#if page.figures?.source == null}
+          <p class="muted">No worker runs it now: the source's detail comes from the node that runs it.</p>
+        {:else if source.length === 0}
+          <p class="muted">
+            Not read yet: the node that runs it reads the source's detail at each heartbeat
+            (its heartbeat, 60 s by default).
+          </p>
+        {:else}
+          <dl>
+            {#each source as s (s.label)}<dt>{s.label}</dt><dd class={s.tone}>{s.value}</dd>{/each}
+          </dl>
+        {/if}
+      </section>
+      <section class="panel">
+        <h3>Snapshot</h3>
+        {#if snap.current !== null}
+          <p><a href={snap.current.href}>{snap.current.job}</a> {snap.current.what}</p>
+          {#if snap.current.note !== null}<p class="muted">"{snap.current.note}"</p>{/if}
+          <p>{snap.current.progress}</p>
+        {:else}
+          <p class="muted">No job runs now.</p>
+        {/if}
+        {#if snap.earlier.length > 0}
+          <p class="muted">
+            Earlier:
+            {#each snap.earlier as j, i (j.job)}{i > 0 ? ' · ' : ''}<a href={j.href}>{j.job}</a> {j.text}{/each}
+            · <a href="/ui/snapshots?pipeline={encodeURIComponent(id)}">all →</a>
+          </p>
+        {/if}
+      </section>
+    </aside>
+  </div>
+
+  <section class="panel">
+    <h3>Activity on this pipeline</h3>
+    <ul class="activity">
+      {#each lines as e (e.key)}
+        <li
+          class:on={e.key === hovered}
+          class={e.severity}
+          onmouseenter={() => (hovered = e.key)}
+          onmouseleave={() => (hovered = null)}
+        >
+          <span class="when">{e.when}</span>
+          <span>{e.text}{#if e.who !== null}<span class="muted"> · {e.who}</span>{/if}{#if e.note !== null}<span class="muted"> "{e.note}"</span>{/if}</span>
+        </li>
+      {:else}
+        <li class="muted">Nothing recorded about it yet.</li>
+      {/each}
+    </ul>
+    <a href="/ui/events?object={encodeURIComponent(id)}">All its events →</a>
+  </section>
+
+  {#if acting !== null && target !== null}
+    {@const v = acting}
+    <Dialog
+      title="{ACTIONS[v].label} {id}?"
+      lines={[ACTIONS[v].effect(target)]}
+      recorded={recordedAs(v, session)}
+      confirm={ACTIONS[v].label}
+      destructive={ACTIONS[v].destructive}
+      {blocked}
+      onConfirm={(note) => act(v, note)}
+      onClose={() => (acting = null)}
+    />
+  {/if}
+  {#if snapping && target !== null}
+    <Dialog
+      title="Snapshot tables of {id}?"
+      lines={[
+        `A job copies the chosen tables again from the source; ${id} restarts${target.node === null ? '' : ` on ${target.node}`} to take it, then keeps streaming.`,
+        truncate
+          ? 'Truncate empties each table first: lake readers see it empty or partly copied until its copy ends.'
+          : 'Without truncate, lake readers see each table as it is while it copies; rows deleted at the source meanwhile stay in Delta.',
+      ]}
+      recorded="snapshot started by {session.name} ({session.role})"
+      confirm={chosen.length === 1 ? 'Copy 1 table' : `Copy ${chosen.length} tables`}
+      blocked={blocked ?? (chosen.length === 0 ? 'Choose the tables to copy.' : null)}
+      onConfirm={snapshot}
+      onClose={() => (snapping = false)}
+    >
+      <fieldset class="pick">
+        <legend>Tables</legend>
+        {#each tables as t (t.table)}
+          <label><input type="checkbox" checked={chosen.includes(t.table)} onchange={() => pick(t.table)} /> {t.table}</label>
+        {/each}
+      </fieldset>
+      <label><input type="checkbox" bind:checked={truncate} /> Truncate each table first</label>
+    </Dialog>
+  {/if}
+{/if}
+
+<style>
+  h2 {
+    display: flex;
+    gap: 10px;
+    align-items: center;
+  }
+  .where {
+    margin: -6px 0 10px;
+  }
+  .error {
+    color: var(--red);
+  }
+  .actions {
+    display: flex;
+    gap: 6px;
+    margin-bottom: 12px;
+  }
+  .destructive {
+    color: var(--red);
+  }
+  .panel {
+    background: var(--bg-panel);
+    border: 1px solid var(--border);
+    border-radius: 8px;
+    padding: 10px 12px;
+    margin-bottom: 12px;
+  }
+  .panel h3 {
+    margin: 0 0 8px;
+    font-size: 14px;
+  }
+  .cols {
+    display: grid;
+    grid-template-columns: 2fr 1fr;
+    gap: 12px;
+  }
+  table {
+    width: 100%;
+    border-collapse: collapse;
+  }
+  th,
+  td {
+    text-align: left;
+    padding: 4px 6px;
+    border-bottom: 1px solid var(--border);
+  }
+  .schema {
+    color: var(--text-2);
+  }
+  dl {
+    display: grid;
+    grid-template-columns: auto 1fr;
+    gap: 4px 10px;
+    margin: 0;
+  }
+  dt {
+    color: var(--text-2);
+  }
+  dd {
+    margin: 0;
+  }
+  dd.orange {
+    color: var(--orange);
+  }
+  dd.red {
+    color: var(--red);
+  }
+  .activity {
+    list-style: none;
+    padding: 0;
+    margin: 0 0 8px;
+  }
+  .activity li {
+    display: flex;
+    gap: 10px;
+    padding: 3px 4px;
+    border-radius: 4px;
+  }
+  .activity li.on {
+    background: var(--bg);
+    outline: 1px solid var(--accent);
+  }
+  .activity li.error .when {
+    color: var(--red);
+  }
+  .when {
+    color: var(--text-2);
+    min-width: 80px;
+    font-variant-numeric: tabular-nums;
+  }
+  .chip {
+    font-size: 12px;
+    font-weight: normal;
+    padding: 1px 8px;
+    border-radius: 10px;
+    background: var(--grey-bg);
+  }
+  .chip.green {
+    background: var(--green-bg);
+  }
+  .chip.orange {
+    background: var(--orange-bg);
+  }
+  .chip.red {
+    background: var(--red-bg);
+  }
+  .notice {
+    padding: 6px 10px;
+    border-radius: 6px;
+    background: var(--grey-bg);
+  }
+  .notice.failing {
+    background: var(--orange-bg);
+  }
+  .stale {
+    opacity: 0.55;
+  }
+  .pick {
+    border: 1px solid var(--border);
+    border-radius: 6px;
+    max-height: 160px;
+    overflow: auto;
+    margin: 8px 0;
+  }
+  .pick label {
+    display: block;
+  }
+</style>
````

`ui/src/pipelines/detail.ts`:

````diff
diff --git a/ui/src/pipelines/detail.ts b/ui/src/pipelines/detail.ts
new file mode 100644
index 0000000000000000000000000000000000000000..5e8c16082c00157cf6a158b2b5da251ebf420243
--- /dev/null
+++ b/ui/src/pipelines/detail.ts
@@ -0,0 +1,279 @@
+// A pipeline's page (mockup 06), worked out from what the page read and the stream sent: the
+// header, the tables, the source, the snapshot, the chart's events and the activity.
+
+import { ago, bytes, clock, duration } from '../lib/format';
+import { compact, formatValue, type Mark } from '../lib/series';
+import { at, route } from '../model/facts';
+import type { ClusterStatus, EventItem, Figures, JobDetail, PipelineDetail, PipelineItem } from '../model/types';
+import { shortJob, walPast } from '../overview/attention';
+import type { Tone } from '../overview/derive';
+
+/** The events read for the page, and the newest shown in its activity. */
+export const ACTIVITY_READ = 50;
+export const ACTIVITY_SHOWN = 10;
+/** A table's schema change shows for this long. */
+export const SCHEMA_SHOWN_S = 86_400;
+
+/** A position as people read it: `3/A41C2B10`, its dialect's tag dropped. */
+export function position(tagged: string | null | undefined): string | null {
+  if (tagged === null || tagged === undefined) return null;
+  const i = tagged.indexOf(':');
+  return i === -1 ? tagged : tagged.slice(i + 1);
+}
+
+const plural = (n: number, one: string) => `${n} ${one}${n === 1 ? '' : 's'}`;
+
+export interface Header {
+  state: { label: string; tone: Tone };
+  route: string;
+  /** Where it runs and its lease: `on n2 · away from owner n1 · lease epoch 7, expires in 8 s`. */
+  where: string;
+}
+
+export function header(
+  d: PipelineDetail | null,
+  item: PipelineItem | undefined,
+  fig: Figures | null,
+  lease: ClusterStatus['pipelines'][number] | undefined,
+  now: number,
+): Header | null {
+  const p = d ?? item;
+  if (p === undefined) return null;
+  const halted = fig !== null ? fig.tables.filter((t) => t.halt !== null).length : (d?.halted_tables.length ?? 0);
+  const actual = fig?.actual ?? p.actual_state;
+  const state: Header['state'] =
+    halted > 0
+      ? { label: `${plural(halted, 'table')} halted`, tone: 'red' }
+      : actual === 'error'
+        ? { label: 'error', tone: 'red' }
+        : { label: actual ?? 'unknown', tone: actual === 'running' ? 'green' : 'grey' };
+  const node = fig?.node ?? d?.holder?.node ?? item?.holder ?? null;
+  const parts = [node === null ? 'no node holds its lease' : `on ${node}`];
+  if (node !== null && lease?.ring_owner != null && lease.ring_owner !== node) {
+    parts.push(`away from owner ${lease.ring_owner}`);
+  }
+  const epoch = fig?.owner_epoch ?? d?.holder?.owner_epoch ?? lease?.owner_epoch;
+  if (node !== null && epoch !== undefined && epoch !== null) {
+    const ends = at(lease?.expires_at);
+    parts.push(`lease epoch ${epoch}${ends === null ? '' : `, expires in ${duration((ends - now) / 1000)}`}`);
+  }
+  return { state, route: route(p, true), where: parts.join(' · ') };
+}
+
+export interface TableLine {
+  table: string;
+  href: string;
+  state: { label: string; tone: Tone };
+  changes: number | null;
+  lastCommit: number | null;
+  version: number | null;
+  /** The last MERGE's files rewritten, of the table's. */
+  files: string | null;
+  schema: string;
+}
+
+const STATES: Record<string, { label: string; tone: Tone }> = {
+  streaming: { label: 'running', tone: 'green' },
+  halted: { label: 'halted', tone: 'red' },
+  snapshot: { label: 'snapshot', tone: 'green' },
+  copied: { label: 'copied', tone: 'grey' },
+};
+
+/** Each table, halted first: from the worker's figures, else the halts storage keeps. */
+export function tableLines(
+  id: string,
+  fig: Figures | null,
+  d: PipelineDetail | null,
+  events: EventItem[],
+  now: number,
+): TableLine[] {
+  const tables =
+    fig?.tables ??
+    (d?.halted_tables ?? []).map((h) => ({
+      table: h.table,
+      state: 'halted',
+      halt: { code: h.code, detail: h.detail, since: h.since },
+      changes_per_minute: 0,
+      last_commit: null,
+      version: null,
+      files_rewritten: null,
+      files: null,
+      soft_dropped: 0,
+      stale: false,
+    }));
+  return tables
+    .map((t) => {
+      const change = events.find(
+        (e) =>
+          e.kind.startsWith('schema.') && e.object === t.table && now - (at(e.at) ?? 0) < SCHEMA_SHOWN_S * 1000,
+      );
+      const schema =
+        t.halt !== null
+          ? t.halt.detail
+          : change !== undefined
+            ? change.text
+            : t.soft_dropped > 0
+              ? `${plural(t.soft_dropped, 'soft-dropped column')}`
+              : 'unchanged';
+      const state = t.stale ? { label: 'stale', tone: 'orange' as Tone } : (STATES[t.state] ?? { label: t.state, tone: 'grey' as Tone });
+      return {
+        table: t.table,
+        href: `/ui/tables/${encodeURIComponent(id)}/${encodeURIComponent(t.table)}`,
+        state,
+        changes: fig === null ? null : t.changes_per_minute,
+        lastCommit: at(t.last_commit),
+        version: t.version,
+        files: t.files_rewritten === null ? null : `${t.files_rewritten}${t.files === null ? '' : ` of ${t.files}`}`,
+        schema,
+      };
+    })
+    .sort((a, b) => Number(b.state.label === 'halted') - Number(a.state.label === 'halted') || a.table.localeCompare(b.table));
+}
+
+export interface SourceLine {
+  label: string;
+  value: string;
+  tone: Tone;
+}
+
+/** What the source keeps for the pipeline: its slot and publication, or its binlog position. */
+export function sourceLines(fig: Figures | null): SourceLine[] {
+  const s = fig?.source;
+  const d = s?.detail;
+  if (s === null || s === undefined || d === null || d === undefined) return [];
+  const line = (label: string, value: string, tone: Tone = 'normal') => ({ label, value, tone });
+  if (d.engine === 'postgres') {
+    const slot = d.slot ?? null;
+    const held = slot?.wal_held_bytes ?? null;
+    const max = d.max_slot_wal_keep_size ?? null;
+    return [
+      slot === null
+        ? line('Slot', 'none by its name', 'red')
+        : line('Slot', `${slot.name} ${slot.active ? 'active' : 'inactive'}`, slot.active ? 'normal' : 'orange'),
+      line(
+        'WAL held',
+        held === null ? '—' : max === null ? `${bytes(held)} · no max_slot_wal_keep_size` : `${bytes(held)} of max_slot_wal_keep_size ${bytes(max)}`,
+        held !== null && walPast(held, max) ? 'orange' : 'normal',
+      ),
+      line('Confirmed LSN', position(slot?.confirmed_flush) ?? '—'),
+      d.publication == null
+        ? line('Publication', 'none by its name', 'red')
+        : line('Publication', `${d.publication.name} · ${plural(d.publication.tables, 'table')}`),
+    ];
+  }
+  return [
+    line('Position', position(d.head) ?? '—'),
+    line('Binlogs kept', d.binlog_expire_seconds == null ? 'never expire' : duration(d.binlog_expire_seconds)),
+    line('Behind', s.behind_seconds == null ? '—' : formatValue('seconds', s.behind_seconds)),
+  ];
+}
+
+export interface Snapshot {
+  current: { job: string; href: string; what: string; note: string | null; progress: string } | null;
+  earlier: { job: string; href: string; text: string }[];
+}
+
+const jobHref = (id: string) => `/ui/snapshots/${encodeURIComponent(id)}`;
+
+/** The running (or queued) job with its progress, and the three ended last. */
+export function snapshotPanel(jobs: JobDetail[], now: number): Snapshot {
+  const newest = [...jobs].sort((a, b) => (at(b.requested_at) ?? 0) - (at(a.requested_at) ?? 0));
+  const open = newest.find((j) => j.finished_at === null);
+  let current: Snapshot['current'] = null;
+  if (open !== undefined) {
+    const names = open.tables.map((t) => t.table);
+    const tables = names.length <= 2 ? names.join(', ') : `${names.length} tables`;
+    const progress =
+      open.status === 'running'
+        ? [
+            open.estimate ? `${Math.min(99, Math.floor((open.rows_done / open.estimate) * 100))}%` : null,
+            `${compact(open.rows_done)}${open.estimate ? ` of ~${compact(open.estimate)}` : ''} rows`,
+            open.eta_seconds === null ? null : `ETA ${duration(open.eta_seconds)}`,
+            open.rate === null ? null : `${compact(open.rate)} rows/s`,
+          ]
+            .filter((p) => p !== null)
+            .join(' · ')
+        : open.status === 'queued'
+          ? 'queued: the pipeline restarts to take it'
+          : open.status;
+    current = {
+      job: shortJob(open.job_id),
+      href: jobHref(open.job_id),
+      what: `${tables}${open.truncate ? ' · truncate' : ''} · started by ${open.requested_by} ${clock(at(open.requested_at) ?? now)}`,
+      note: open.note,
+      progress,
+    };
+  }
+  const earlier = newest
+    .filter((j) => j.finished_at !== null)
+    .slice(0, 3)
+    .map((j) => {
+      const when = ago(at(j.finished_at)!, now);
+      const how =
+        j.status === 'completed'
+          ? `✓ ${when}`
+          : j.status === 'failed'
+            ? `✕ failed ${when}`
+            : `cancelled${j.cancel === null ? '' : ` by ${j.cancel.by}`} ${when}`;
+      return { job: shortJob(j.job_id), href: jobHref(j.job_id), text: how };
+    });
+  return { current, earlier };
+}
+
+const ACTED: Record<string, string> = {
+  'action.pipeline.restart': 'restarted',
+  'action.pipeline.pause': 'paused',
+  'action.pipeline.resume': 'resumed',
+  'action.pipeline.stop': 'stopped',
+  'action.pipeline.flush': 'flushed',
+};
+
+/** An event's label on the chart, `null` for one it doesn't draw. */
+export function markOf(e: EventItem): string | null {
+  if (e.kind in ACTED) return e.outcome === 'accepted' ? ACTED[e.kind] : null;
+  if (e.kind === 'action.snapshot.start') return e.outcome === 'accepted' ? 'snapshot' : null;
+  if (e.kind === 'job.completed') return 'snapshot done';
+  if (e.kind === 'job.failed') return 'snapshot failed';
+  if (e.kind === 'table.halted') return `${e.object ?? 'a table'} halted`;
+  if (e.kind === 'lease.takeover') return 'failover';
+  if (e.kind === 'lease.lost') return 'lease lost';
+  if (e.kind === 'pipeline.error') return 'error';
+  return null;
+}
+
+/** The events the chart draws between `since` and now, unix seconds. */
+export function marks(events: EventItem[], since: number): Mark[] {
+  return events.flatMap((e) => {
+    const label = markOf(e);
+    const t = at(e.at);
+    return label === null || t === null || t / 1000 < since ? [] : [{ key: e.key, at: t / 1000, label }];
+  });
+}
+
+export interface ActivityLine {
+  key: string;
+  when: string;
+  text: string;
+  /** `Oleksii (operator)`, for a person's or a token's action. */
+  who: string | null;
+  note: string | null;
+  severity: EventItem['severity'];
+}
+
+/** The newest events, a line each: today's by their time, older ones by their date too. */
+export function activity(events: EventItem[], now: number): ActivityLine[] {
+  const today = new Date(now).toDateString();
+  return events.slice(0, ACTIVITY_SHOWN).map((e) => {
+    const t = at(e.at) ?? now;
+    const d = new Date(t);
+    const date = `${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
+    return {
+      key: e.key,
+      when: d.toDateString() === today ? clock(t) : `${date} ${clock(t)}`,
+      text: e.text,
+      who: e.actor === undefined ? null : `${e.actor.name} (${e.actor.role})`,
+      note: e.note ?? null,
+      severity: e.severity,
+    };
+  });
+}
````

`ui/src/pipelines/page.svelte.ts`:

````diff
diff --git a/ui/src/pipelines/page.svelte.ts b/ui/src/pipelines/page.svelte.ts
new file mode 100644
index 0000000000000000000000000000000000000000..e2207305bd73076df02302bb9907f477432ed815
--- /dev/null
+++ b/ui/src/pipelines/page.svelte.ts
@@ -0,0 +1,141 @@
+// A pipeline's page (Admin_Console §4, §8; mockup 06): what the console knows already is shown at
+// once (its row, the figures the stream sent), then each part is read on its own and followed
+// through the stream. A part that can't be read keeps what it had and says why.
+
+import { HttpError, type Client } from '../lib/api';
+import type { Live } from '../lib/live.svelte';
+import { REREAD_AFTER_MS, type PipelineMsg, type Read } from '../model/model.svelte';
+import type { EventItem, Figures, JobDetail, PipelineDetail } from '../model/types';
+import { ACTIVITY_READ } from './detail';
+import { LagHistory } from './lag.svelte';
+
+export type PagePart = 'detail' | 'live' | 'jobs' | 'events';
+export const PAGE_PARTS: PagePart[] = ['detail', 'live', 'jobs', 'events'];
+
+/** The parts an event about the pipeline changes. */
+function partsOf(kind: string): PagePart[] {
+  if (kind.startsWith('job.') || kind.startsWith('action.snapshot.')) return ['jobs'];
+  if (kind.startsWith('table.') || kind.startsWith('schema.')) return ['detail', 'live'];
+  if (kind.startsWith('pipeline.') || kind.startsWith('lease.') || kind.startsWith('action.pipeline.')) {
+    return ['detail'];
+  }
+  return [];
+}
+
+export class PipelinePage {
+  readonly id: string;
+  detail = $state<PipelineDetail | null>(null);
+  figures = $state<Figures | null>(null);
+  jobs = $state<JobDetail[]>([]);
+  /** The newest events about it, newest first. */
+  events = $state<EventItem[]>([]);
+  read = $state<Record<PagePart, Read>>(
+    Object.fromEntries(PAGE_PARTS.map((p) => [p, { at: null, error: null }])) as Record<PagePart, Read>,
+  );
+  /** The generation this node runs has no such pipeline. */
+  missing = $state(false);
+  readonly lag: LagHistory;
+
+  #api: Client;
+  #path: string;
+  #pending = new Set<PagePart>();
+  #timer: ReturnType<typeof setTimeout> | undefined;
+
+  /** `seed`: the figures the stream sent already, shown before the page's own read. */
+  constructor(api: Client, id: string, seed: Figures | null = null, now: () => number = Date.now) {
+    this.#api = api;
+    this.id = id;
+    this.#path = `/v1/pipelines/${encodeURIComponent(id)}`;
+    this.figures = seed;
+    this.lag = new LagHistory(api, id, now);
+  }
+
+  /** Reads every part, and the lag's 6 hours. */
+  async load(): Promise<void> {
+    await Promise.all([this.#loadParts(PAGE_PARTS), this.lag.load()]);
+  }
+
+  async #loadParts(parts: PagePart[]): Promise<void> {
+    await Promise.all(parts.map((p) => this.#loadPart(p)));
+  }
+
+  /** Follows `live` for this pipeline; returns the way to stop. */
+  follow(live: Live): () => void {
+    const stops = [
+      // The lag's line reads itself again.
+      live.onResync(() => void this.#loadParts(PAGE_PARTS)),
+      live.on('figures', (d) => this.figuresMsg(d as Figures)),
+      live.on('pipeline', (d) => this.pipelineMsg(d as PipelineMsg)),
+      live.on('event', (d) => this.event(d as EventItem)),
+      this.lag.follow(live),
+    ];
+    return () => {
+      for (const stop of stops) stop();
+      clearTimeout(this.#timer);
+    };
+  }
+
+  /** Its figures, from the node that runs it: the newest owner's win. */
+  figuresMsg(m: Figures): void {
+    if (m.id !== this.id) return;
+    if (this.figures !== null && this.figures.owner_epoch > m.owner_epoch) return;
+    this.figures = m;
+  }
+
+  pipelineMsg(m: PipelineMsg): void {
+    if (m.pipeline !== this.id || m.actual === 'standby') return;
+    const f = this.figures;
+    if (f !== null && f.owner_epoch > m.owner_epoch) return;
+    if (f !== null) {
+      f.actual = m.actual;
+      f.error = m.error;
+      f.node = m.node;
+    }
+    this.#reread(['detail']);
+  }
+
+  event(e: EventItem): void {
+    if (e.pipeline !== this.id || this.events.some((x) => x.key === e.key)) return;
+    this.events = [e, ...this.events].slice(0, ACTIVITY_READ);
+    this.#reread(partsOf(e.kind));
+  }
+
+  #reread(parts: PagePart[]): void {
+    for (const p of parts) this.#pending.add(p);
+    if (this.#timer !== undefined || this.#pending.size === 0) return;
+    this.#timer = setTimeout(() => {
+      this.#timer = undefined;
+      const parts = [...this.#pending];
+      this.#pending.clear();
+      void this.#loadParts(parts);
+    }, REREAD_AFTER_MS);
+  }
+
+  async #loadPart(part: PagePart): Promise<void> {
+    try {
+      switch (part) {
+        case 'detail':
+          this.detail = await this.#api.get<PipelineDetail>(this.#path);
+          this.missing = false;
+          break;
+        case 'live':
+          this.figuresMsg(await this.#api.get<Figures>(`${this.#path}/live`));
+          break;
+        case 'jobs':
+          this.jobs = (await this.#api.get<{ jobs: JobDetail[] }>(`${this.#path}/snapshots`)).jobs;
+          break;
+        case 'events':
+          this.events = (
+            await this.#api.get<{ events: EventItem[] }>(
+              `/v1/events?object=${encodeURIComponent(this.id)}&limit=${ACTIVITY_READ}`,
+            )
+          ).events;
+          break;
+      }
+      this.read[part] = { at: Date.now(), error: null };
+    } catch (e) {
+      if (part === 'detail' && e instanceof HttpError && e.status === 404) this.missing = true;
+      this.read[part] = { at: this.read[part].at, error: e instanceof Error ? e.message : String(e) };
+    }
+  }
+}
````

- [ ] **Step 4: Run the tests and the gates**

Run the step's tests again, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
(cd ui && npm run check && npm test && npm run build)
```

`npm run build` now writes uPlot to `app/uPlot.esm-<hash>.js`, apart from the app's own file.

Expected: no output from the first three; every test passes; svelte-check reports 0 errors and
0 warnings.

- [ ] **Step 5: Commit**

```bash
git add -A ui/src/App.svelte ui/src/model/types.ts ui/src/pipelines/Pipeline.svelte ui/src/pipelines/Pipeline.test.ts ui/src/pipelines/detail.test.ts ui/src/pipelines/detail.ts ui/src/pipelines/fixtures.ts ui/src/pipelines/page.svelte.ts ui/src/pipelines/page.test.ts
git commit -m "feat(ui): a pipeline's page — header and lease, actions, the lag chart and its events, tables, source, snapshot, activity"
```

---

### Task 6: In a browser: the pipelines' flows and the navigation budget (`ui/e2e`, the E2E suite, the site)

**Files:**
- Modify: `crates/data-avalanche/tests/e2e/console_app.rs`
- Modify: `docs/src/content/docs/deployment/console.md`
- Modify: `ui/e2e/basic.spec.ts`
- Modify: `ui/e2e/budget.spec.ts`
- Modify: `ui/e2e/jwt.spec.ts`
- Create: `ui/e2e/pipelines.spec.ts`

**Interfaces:**
- Consumes: Tasks 4 and 5's pages; M15's `console_app.rs` (`node`, `playwright`, `console`,
  `require_browser!`) and `ui/e2e/common.ts` (`env`, `signIn`).
- Produces: `the_pipelines_pages_and_their_actions_in_a_browser` (`pipelines.spec.ts`), the jwt
  flow's viewer case (`DA_PIPELINE` passed to it), and `the_pages_are_drawn_within_their_budgets`
  (M15's `the_first_page_is_drawn_within_its_budget`, now with the moves, D-M16a.10).

- [ ] **Step 1: Write the failing tests**

The flows: `pipelines.spec.ts` under `basic` (the list's filters in the address, a row opened from
the keyboard, Back keeping the view; a pipeline's page with its tables, its source, the chart drawn
under the CSP, a pause and a resume through their dialogs recorded with the note; an action on a
selection); `jwt.spec.ts`'s viewer sees the pipelines with nothing to act on; `budget.spec.ts`
times four moves; `basic.spec.ts` finds the Pipelines page built.

`crates/data-avalanche/tests/e2e/console_app.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/console_app.rs b/crates/data-avalanche/tests/e2e/console_app.rs
index d24fc9eebe9a32b4982d422d6a9208405c1cf5c4..adab0ccd48977923c88dbc472d5d00d2073bc0e5 100644
--- a/crates/data-avalanche/tests/e2e/console_app.rs
+++ b/crates/data-avalanche/tests/e2e/console_app.rs
@@ -1,6 +1,6 @@
-//! The console's app in a browser (Admin_Console §10; M15): Playwright's flows (`ui/e2e`) against
-//! a node under each `globals.api.auth` mode, the console switched off, and the first page drawn
-//! within its budget (§8) with 50 pipelines on three nodes. Needs `DATA_AVALANCHE_BROWSER=1`, the
+//! The console's app in a browser (Admin_Console §10; M15, M16a): Playwright's flows (`ui/e2e`)
+//! against a node under each `globals.api.auth` mode, the console switched off, the pipelines'
+//! pages and their actions, and the page budgets (§8) with 50 pipelines on three nodes. Needs `DATA_AVALANCHE_BROWSER=1`, the
 //! app built into the binary (`npm ci && npm run build` in `ui/`, then the binary) and Playwright's
 //! Chromium (`npx playwright install chromium` in `ui/`); without the variable each skips itself.
 
@@ -115,8 +115,8 @@ async fn oauth2_sign_in_in_a_browser() {
     e.cleanup().await;
 }
 
-/// `jwt`: the proxy's token on every request; without one, "No valid token"; one whose roles
-/// map to none, "Forbidden".
+/// `jwt`: the proxy's token on every request, a viewer's showing the pipelines with nothing to act
+/// on; without one, "No valid token"; one whose roles map to none, "Forbidden".
 #[tokio::test]
 async fn jwt_tokens_in_a_browser() {
     require_it!();
@@ -132,6 +132,7 @@ async fn jwt_tokens_in_a_browser() {
     let env = [
         ("DA_VIEWER_TOKEN", viewer.as_str()),
         ("DA_UNMAPPED_TOKEN", unmapped.as_str()),
+        ("DA_PIPELINE", e.test.as_str()),
     ];
     playwright("jwt.spec.ts", p.port(), &env).await;
     assert!(p.terminate().await.success());
@@ -153,9 +154,33 @@ async fn no_authentication_and_the_console_off_in_a_browser() {
     e.cleanup().await;
 }
 
-/// Admin_Console §8: the Overview drawn under 1 s with 50 pipelines on three nodes.
+/// The pipelines' pages under `basic` (mockups 05, 06): the list's filters in the address, kept by
+/// Back; a row opened from the keyboard; a pipeline's tables, source and lag chart; a pause and a
+/// resume through their dialogs, recorded with the note; an action on a selection.
+#[tokio::test]
+async fn the_pipelines_pages_and_their_actions_in_a_browser() {
+    require_it!();
+    require_browser!();
+    let (e, p) = node(
+        "console_app_pipelines",
+        ", auth: basic, user: ops, password: s3cret",
+        &console(""),
+    )
+    .await;
+    let env = [
+        ("DA_USER", "ops"),
+        ("DA_PASSWORD", "s3cret"),
+        ("DA_PIPELINE", e.test.as_str()),
+    ];
+    playwright("pipelines.spec.ts", p.port(), &env).await;
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
+
+/// Admin_Console §8: the Overview drawn under 1 s, and moving between pages under 300 ms, with 50
+/// pipelines on three nodes.
 #[tokio::test]
-async fn the_first_page_is_drawn_within_its_budget() {
+async fn the_pages_are_drawn_within_their_budgets() {
     require_it!();
     require_browser!();
     let more: String = (1..PIPELINES)
@@ -180,18 +205,21 @@ async fn the_first_page_is_drawn_within_its_budget() {
     for i in 1..PIPELINES {
         c.e.drop_source(&format!("_p{i:02}")).await;
     }
-    // Every pipeline runs on a node; whether the ring has balanced them yet doesn't matter here.
     for i in 0..c.nodes.len() {
         c.start(i).await;
     }
+    // Every pipeline runs on its ring owner: a pipeline still being handed over has no tables to
+    // draw until its new worker runs, which would time the hand-off, not the page.
     eventually(
-        "every pipeline runs on a node",
+        "every pipeline runs on its ring owner",
         Duration::from_secs(180),
         async || {
             let (_, v) = c.proc("n1").call("GET", "/v1/cluster/status").await;
-            v["pipelines"]
-                .as_array()
-                .is_some_and(|p| p.len() == PIPELINES && p.iter().all(|l| l["holder"].is_string()))
+            v["pipelines"].as_array().is_some_and(|p| {
+                p.len() == PIPELINES
+                    && p.iter()
+                        .all(|l| l["holder"].is_string() && l["holder"] == l["ring_owner"])
+            })
         },
     )
     .await;
````

`ui/e2e/basic.spec.ts`:

````diff
diff --git a/ui/e2e/basic.spec.ts b/ui/e2e/basic.spec.ts
index 4ab653238e41fa1f63b735c9eda4fee8f844b8a4..cab802e047595fe7a0058978e4574f38d5ebaa2f 100644
--- a/ui/e2e/basic.spec.ts
+++ b/ui/e2e/basic.spec.ts
@@ -13,7 +13,7 @@ test('a page asked for without a session goes to the sign-in, then back to it',
   await signIn(page, user, password);
   await expect(page).toHaveURL(/\/ui\/pipelines$/);
   await expect(page.locator('nav [aria-current="page"]')).toHaveText('Pipelines');
-  await expect(page.locator('main')).toContainText("This page isn't in this build yet.");
+  await expect(page.locator('h2')).toHaveText('Pipelines');
   await page.click('nav >> text=Overview');
   await expect(page).toHaveURL(/\/ui\/$/);
   await overview(page, user, 'admin');
````

`ui/e2e/budget.spec.ts`:

````diff
diff --git a/ui/e2e/budget.spec.ts b/ui/e2e/budget.spec.ts
index 10bd2aa92a9dd31156cf697e110e763c2fad0774..83a2439c87fac055bc51f841fdbb18df12515902 100644
--- a/ui/e2e/budget.spec.ts
+++ b/ui/e2e/budget.spec.ts
@@ -1,8 +1,10 @@
-import { expect, test } from '@playwright/test';
+import { expect, test, type Page } from '@playwright/test';
 import { env } from './common';
 
-/** Admin_Console §8: the first page drawn under 1 s, with 50 pipelines. */
+/** Admin_Console §8: the first page drawn under 1 s, moving between pages under 300 ms, with 50
+ * pipelines. */
 const FIRST_PAGE_MS = 1_000;
+const MOVE_MS = 300;
 
 test(`the Overview is drawn within ${FIRST_PAGE_MS} ms of asking for it`, async ({ page }) => {
   const pipelines = env('DA_PIPELINES');
@@ -24,3 +26,51 @@ test(`the Overview is drawn within ${FIRST_PAGE_MS} ms of asking for it`, async
   console.log(`drawn in ${Math.round(ms)} ms`);
   expect(ms).toBeLessThan(FIRST_PAGE_MS);
 });
+
+/** Clicks `link` in the page, or goes Back without one, and times until `ready` matches `n`
+ * elements, taken in the page at each frame. */
+async function move(page: Page, link: string | null, ready: string, n: number): Promise<number> {
+  return page.evaluate(
+    ({ link, ready, n }) =>
+      new Promise<number>((done) => {
+        const t0 = performance.now();
+        if (link === null) history.back();
+        else (document.querySelector(link) as HTMLElement).click();
+        const check = () =>
+          document.querySelectorAll(ready).length >= n ? done(performance.now() - t0) : requestAnimationFrame(check);
+        requestAnimationFrame(check);
+      }),
+    { link, ready, n },
+  );
+}
+
+test(`moving between pages takes under ${MOVE_MS} ms`, async ({ page }) => {
+  const pipelines = Number(env('DA_PIPELINES'));
+  await page.goto('/ui/');
+  await expect(page.locator('tbody tr')).toHaveCount(pipelines);
+  await expect(page.locator('.conn')).toHaveText('live', { timeout: 15_000 });
+  // A pipeline's page draws its tables from the figures the stream sent; until they come (each
+  // node sends its pipelines' every 5 s, once the page follows it) it reads them from the node that
+  // runs it. The moves are timed once the stream has sent every pipeline's.
+  await page.evaluate(
+    (n) =>
+      new Promise<void>((done, fail) => {
+        const ids = new Set<string>();
+        const es = new EventSource('/v1/stream?kind=figures');
+        const end = setTimeout(() => (es.close(), fail(new Error(`figures for ${ids.size} of ${n}`))), 30_000);
+        es.addEventListener('figures', (e) => {
+          ids.add(JSON.parse((e as MessageEvent).data).id);
+          if (ids.size === n) (clearTimeout(end), es.close(), done());
+        });
+      }),
+    pipelines,
+  );
+  const moves = [
+    ['the Overview to the pipelines', await move(page, 'nav a[href="/ui/pipelines"]', 'tbody a.open', pipelines)],
+    ['the pipelines to a pipeline', await move(page, 'tbody a.open[href="/ui/pipelines/p01"]', '.cols tbody td a', 1)],
+    ['a pipeline back to the pipelines', await move(page, null, 'tbody a.open', pipelines)],
+    ['the pipelines to the Overview', await move(page, 'nav a[href="/ui/"]', '.tile', 5)],
+  ] as const;
+  for (const [what, ms] of moves) console.log(`${what}: ${Math.round(ms)} ms`);
+  for (const [what, ms] of moves) expect(ms, what).toBeLessThan(MOVE_MS);
+});
````

`ui/e2e/jwt.spec.ts`:

````diff
diff --git a/ui/e2e/jwt.spec.ts b/ui/e2e/jwt.spec.ts
index f01b670b8e1ca36961dfa2d199d46410ff88672b..de36f13f25870fd42cd106bbc76d9e0ebdd1b999 100644
--- a/ui/e2e/jwt.spec.ts
+++ b/ui/e2e/jwt.spec.ts
@@ -2,6 +2,7 @@ import { expect, test } from '@playwright/test';
 import { env, overview } from './common';
 
 const header = (token: string) => ({ Authorization: `Bearer ${token}` });
+const pipeline = env('DA_PIPELINE');
 
 test.describe('with the proxy sending a viewer token', () => {
   test.use({ extraHTTPHeaders: header(env('DA_VIEWER_TOKEN')) });
@@ -10,6 +11,17 @@ test.describe('with the proxy sending a viewer token', () => {
     await overview(page, 'alice', 'viewer');
     await expect(page.locator('form[action="/ui/sign-out"]')).toHaveCount(0);
   });
+
+  test('the pipelines are shown with nothing to select or act on', async ({ page }) => {
+    await page.goto('/ui/pipelines');
+    const open = page.locator(`tbody a.open[href="/ui/pipelines/${pipeline}"]`);
+    await expect(open).toBeVisible();
+    await expect(page.locator('input[type="checkbox"]')).toHaveCount(0);
+    await open.click();
+    await expect(page.locator('h2')).toContainText(pipeline);
+    await expect(page.locator('.where')).toBeVisible();
+    await expect(page.locator('.actions')).toHaveCount(0);
+  });
 });
 
 test.describe('with a token whose roles map to none', () => {
````

`ui/e2e/pipelines.spec.ts`:

````diff
diff --git a/ui/e2e/pipelines.spec.ts b/ui/e2e/pipelines.spec.ts
new file mode 100644
index 0000000000000000000000000000000000000000..401192a382cc0ab7d01660ef286454fb47ee5e1c
--- /dev/null
+++ b/ui/e2e/pipelines.spec.ts
@@ -0,0 +1,69 @@
+import { expect, test, type Page } from '@playwright/test';
+import { env, signIn } from './common';
+
+const user = env('DA_USER');
+const password = env('DA_PASSWORD');
+const pipeline = env('DA_PIPELINE');
+
+/** Signs in on the way to `path`, and waits for the page to be live. */
+async function signedIn(page: Page, path: string): Promise<void> {
+  await page.goto(path);
+  await signIn(page, user, password);
+  await expect(page.locator('.conn')).toHaveText('live', { timeout: 15_000 });
+}
+
+const dialog = (page: Page) => page.locator('[role="dialog"]');
+
+test('the list keeps its filters in the address, opens a row from the keyboard, and Back keeps them', async ({
+  page,
+}) => {
+  await signedIn(page, '/ui/pipelines');
+  await expect(page.locator('h2')).toHaveText('Pipelines');
+  const row = page.locator('tbody tr', { has: page.locator(`a.open[href="/ui/pipelines/${pipeline}"]`) });
+  await expect(row).toContainText(" → Δ ");
+  await expect(row.locator('.chip')).toHaveText('running', { timeout: 15_000 });
+  await page.click('.chips >> text=Paused');
+  await expect(page).toHaveURL(/\/ui\/pipelines\?show=paused$/);
+  await expect(page.locator('tbody')).toContainText('No pipeline shown');
+  await page.click('.chips >> text=Running');
+  await expect(page).toHaveURL(/\/ui\/pipelines\?show=running$/);
+  await row.locator('a.open').focus();
+  await page.keyboard.press('Enter');
+  await expect(page).toHaveURL(new RegExp(`/ui/pipelines/${pipeline}$`));
+  await expect(page.locator('h2')).toContainText(pipeline);
+  await page.goBack();
+  await expect(page).toHaveURL(/\/ui\/pipelines\?show=running$/);
+  await expect(page.locator('.chips [aria-current="true"]')).toContainText('Running');
+});
+
+test("a pipeline's page: its tables, source and lag chart; paused and resumed through their dialogs, each recorded with its note", async ({
+  page,
+}) => {
+  await signedIn(page, `/ui/pipelines/${pipeline}`);
+  await expect(page.locator('.where')).toContainText('PostgreSQL');
+  await expect(page.locator(`a[href^="/ui/tables/${pipeline}/"]`).first()).toBeVisible();
+  // The worker reads its slot at its first heartbeat (60 s by default): until then the page says so.
+  await expect(page.locator('aside')).toContainText(/Slot|Not read yet/);
+  // uPlot draws under the console's Content-Security-Policy.
+  await expect(page.locator('figure.chart canvas')).toBeVisible();
+  await page.click('.actions >> text=Pause');
+  await expect(dialog(page)).toContainText(`Recorded as: paused by ${user} (admin)`);
+  await page.fill('[role="dialog"] textarea', 'maintenance window');
+  await page.click('[role="dialog"] button.go');
+  await expect(dialog(page)).toHaveCount(0);
+  await expect(page.locator('h2 .chip')).toHaveText('paused', { timeout: 15_000 });
+  await expect(page.locator('.activity')).toContainText('"maintenance window"');
+  await page.click('.actions >> text=Resume');
+  await page.click('[role="dialog"] button.go');
+  await expect(page.locator('h2 .chip')).toHaveText('running', { timeout: 15_000 });
+});
+
+test('an action on the pipelines selected', async ({ page }) => {
+  await signedIn(page, '/ui/pipelines');
+  await page.check(`input[aria-label="Select ${pipeline}"]`);
+  await page.click('.acting >> text=Flush…');
+  await expect(dialog(page).locator('h3')).toHaveText(`Flush ${pipeline}?`);
+  await page.click('[role="dialog"] button.go');
+  await expect(dialog(page)).toHaveCount(0);
+  await expect(page.locator('.acting')).toHaveCount(0);
+});
````

- [ ] **Step 2: Run them: they pass, on the pages Tasks 4 and 5 built**

Run (with the emulators up, the app built in): `(cd ui && npm run build) && DATA_AVALANCHE_IT=1 DATA_AVALANCHE_BROWSER=1 DATA_AVALANCHE_UI=required cargo test -p data-avalanche --features e2e-hooks --test e2e console_app -- --test-threads=1`
Expected: PASS, six tests: the flows check Tasks 4 and 5's pages, which are built; the budget
prints each move, each under 300 ms (the Overview to the list 17 ms, the list to a pipeline
11–54 ms over five runs, Back 13 ms, the list to the Overview 11 ms). A worker reads its
source's detail at its first heartbeat (60 s by default), so the page's Source panel may say "Not
read yet" when the flow looks; the flow takes either.

- [ ] **Step 3: Implement**

The site's console page says what Pipelines shows and how acting on pipelines works.

`docs/src/content/docs/deployment/console.md`:

````diff
diff --git a/docs/src/content/docs/deployment/console.md b/docs/src/content/docs/deployment/console.md
index b709db52f7c123f08255eb1c6e7e8ec061f60804..5eff60ff124d2e5dc472703d3e830231cb23a0dc 100644
--- a/docs/src/content/docs/deployment/console.md
+++ b/docs/src/content/docs/deployment/console.md
@@ -29,8 +29,8 @@ cookies are `Secure`, which browsers send over plain HTTP only to `localhost`.
 ## What it shows
 
 Every page has the same top bar: the sections, the live connection's state, the cluster's config
-generation, and who is signed in, with their role and the sign-out. This release has the
-**Overview**; the other sections say they aren't in this build yet.
+generation, and who is signed in, with their role and the sign-out. This build has the
+**Overview** and the **Pipelines**; the other sections say they aren't in this build yet.
 
 The page shows the cluster as it is when it opens, then follows the cluster's live updates
 (`GET /v1/stream`) through whichever node served it, so it changes as things happen without a
@@ -60,6 +60,38 @@ page; a later release makes them configurable):
 A node the console can't follow shows its pipelines as last known, marked with how long it has been
 out of reach.
 
+### The pipelines
+
+**Pipelines** lists every pipeline, the worst first: its state, source and sink, the node that runs
+it (`away` when that isn't its owner on the ring, after a failover), its lag over 6 hours against
+the 60 s target, its changes a minute, its last commit, its tables (how many are halted), and its
+snapshot (the running job's progress and ETA, else the last job's outcome). The filters (by state,
+node, source and name) and the sort (a column's heading) are kept in the page's address, so a link
+shows the same view and Back returns to it. A click anywhere on a row opens it; ↑ and ↓ move
+between rows and Enter opens one.
+
+A pipeline's page has its source and sink, its node and lease, its lag over 6 hours with the
+events that marked it (actions, halts, failovers, snapshot jobs) drawn on the chart, its tables
+with their changes, Delta version, files rewritten by the last MERGE and last schema change, what
+its source keeps for it (PostgreSQL's slot, the WAL it holds against `max_slot_wal_keep_size`, the
+confirmed LSN and the publication; MySQL's position, how long the server keeps binlogs, and how far
+behind the stream reads), its snapshot jobs, and what happened to it. Hovering an event on the
+chart highlights it in the list below, and the reverse.
+
+### Acting on pipelines
+
+An `operator` or an `admin` sees the actions a pipeline's state takes: flush, pause, resume,
+restart, stop, and a snapshot of chosen tables (truncated first, or not). Each opens a dialog that
+says what it does and to what, takes an optional note (up to 500 characters), and shows the line
+it will be recorded as: `restarted by Oleksii (operator)`. The server checks the role again and
+records the action, with the note, in the cluster's events, whatever the page shows. A `viewer`
+sees no actions.
+
+On **Pipelines**, select several to act on them together: the dialog names each one and its node,
+and those the action doesn't apply to (a paused pipeline isn't restarted). They are asked one at a
+time, in order; a failure stops the rest, and the dialog says which failed, why, and which weren't
+asked. While the page is offline, actions wait.
+
 ## Signing in
 
 | `globals.api.auth` | What a person sees |
````

- [ ] **Step 4: Run the tests and the gates**

Run the step's tests again, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
(cd ui && npm run check && npm test && npm run build)
```

and `cargo xtask docs --check`, `(cd docs && npm run build)`.

Expected: no output from the first three; every test passes; svelte-check reports 0 errors and
0 warnings.

- [ ] **Step 5: Commit**

```bash
git add -A crates/data-avalanche/tests/e2e/console_app.rs docs/src/content/docs/deployment/console.md ui/e2e/basic.spec.ts ui/e2e/budget.spec.ts ui/e2e/jwt.spec.ts ui/e2e/pipelines.spec.ts
git commit -m "test(e2e): the pipelines' pages and actions in a browser, a viewer's, and moving between pages within 300 ms; the site"
```

---

## Exit

- Every task committed, one commit each, after the plan's commits; the gates pass on the last one
  (`cargo fmt --all --check`, both clippy runs, `cargo test --workspace`, and in `ui/`
  `npm run check`, `npm test`, `npm run build`).
- With the emulators up, the app built in and Chromium installed: the whole E2E suite on
  PostgreSQL 18 with `DATA_AVALANCHE_UI=required DATA_AVALANCHE_BROWSER=1`, the console's flows
  among it, the navigation budget's four moves each under 300 ms.
- `cargo xtask docs --check` and `cd docs && npm run build` pass.
- At the plan's verification (the code above, on `d8fb47a`, each task's red and green commits
  apart, the diffs replayed to the same trees): every Step 2 failure as stated; at Task 6,
  `cargo test --workspace` 1853 passed, both clippy runs and `cargo fmt --all --check` clean, in
  `ui/` svelte-check 0 errors and 0 warnings and 124 tests passed, `cargo xtask docs --check` held
  and the site built; the whole E2E suite on PostgreSQL 18 with the browser flows: 64 of 65 passed
  (968 s), the budget's move to a pipeline taking 379 ms. That was the flow, not the page, twice
  over. It timed the moves after a fixed 6 s, before the stream had sent every pipeline's figures,
  so the page read the pipeline's from the node that runs it (the header drew in 6–16 ms; the
  tables came with that read, 77–102 ms alone); and it timed them while the ring was still
  handing pipelines to their owners, so a pipeline caught mid-hand-off had no tables until its new
  worker ran (one move took 4 283 ms). The flow now waits for every pipeline's figures, and the
  budget's cluster for every pipeline on its ring owner; then the list to a pipeline took 11–14 ms
  four times running, and the console's six flows passed together (the first page in 362 ms, the
  moves 11–54 ms). The rest of the suite is unchanged since its run.
  A worker reads its source's detail at its first heartbeat (60 s by default): the first run found
  the page saying "No worker runs it now" until then, which Task 5 now tells apart ("Not read
  yet").
- Then, on Oleksii's request: the whole-branch review and its fixes, the push, the PR.

## Deviations during execution

- **Task 2, `ui/src/lib/Chart.svelte` (fix commit after the task's own).** The task review found
  two faults in the plan's code, and the fix found a third:
  - the hover stayed on an event after the pointer left the plot (`cursor` returned without
    `onHover(null)`);
  - the data effect called `setData(data, false)`, which keeps the y scale, so new samples or a
    new target could fall off the axis. It now calls `setData(samples, true)`, which re-ranges
    the auto y scale (uPlot 1.6.32 `setData` → `commit`), then sets the x range;
  - that effect read `data` only inside `plot?.setData(…)`. Its first run comes before uPlot
    loads, so it never subscribed to the samples and the chart kept its first draw. It now
    reads `data`, `target`, `since` and `until` before the call.

  `Chart.test.ts` gained a test for each. No later task touches `Chart.svelte` or `series.ts`,
  so their diffs apply as written.
- **Task 4, `ui/src/pipelines/List.svelte` (fix commit after the task's own).** The task review
  found the selection outliving the filters: `chosen` came from every pipeline, so after ticking
  two and filtering one out the toolbar still said "2 selected" and an action reached the hidden
  one. The list now acts on the selected pipelines its filters show (`chosen` = `shown` ∩
  `selected`), counts those, and keeps the hidden ones ticked for when the filter clears.
  `List.test.ts` gained a test. No later task touches `List.svelte` or `rows.ts`.
