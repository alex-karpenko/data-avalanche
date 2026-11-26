---
updated: 2026-11-26
domain: Architecture
tags: [v1.1, implementation-plan, m16b, console, web-ui, svelte, cluster, nodes, node-actions, removals, playwright]
resolves: [DA-API-21]
---

# M16b — The Console's Cluster, Its Nodes and Their Actions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The console's Cluster section (Admin_Console §4, §5, §7, §8; mockups 03, 04 and 11):
the Cluster page, a node's page, a node's leave and shutdown with their dialogs, the pending
removals and their yellow rule (DA-API-21), and the console's own status in the user menu; with
M16a's two parked notices fixed and the Overview's notice worded as every other page's.

**Architecture:**

- **The server** (Task 1, `crates/data-avalanche-api/src/cluster.rs`): `GET /v1/cluster/status`
  adds each member's `started_at` and the cluster's `settings`; a new `GET /v1/cluster/removals`
  (viewer) lists the removal tombstones not done yet (Configuration §12.3).
- **The model** (Task 2, `ui/src/model/`): a `removals` part, each node's own figures from the
  stream, the node the live updates come through; the yellow rule for a removal pending over
  2 min; one notice wording for every page (`lib/notices.ts`), the cluster's read among a
  pipeline page's parts.
- **The Cluster page** (Task 3, `ui/src/cluster/`): tiles, members with their slowest lease
  renewal of the hour, the pipelines away from their owner, the leases, the pending removals.
- **A node's page** (Task 4, `ui/src/nodes/`): header, the pipelines it holds, its renewals,
  memory and CPU over 6 hours, its activity, its gossip; each part read from the node itself.
- **The node actions** (Task 5, `ui/src/nodes/actions.ts`): Leave and Shut down, an admin's,
  through M16a's dialog; the shutdown confirmed by the node's name typed.
- **The console's status** (Task 6, `ui/src/lib/status.ts`, `Frame.svelte`): the user menu's
  table: live updates, nodes answering, the built-in history, Prometheus, the event store.
- **In a browser** (Task 7): the Cluster flows on three nodes (a leave, then a shutdown), two
  more budget moves, the site's console page.

**Tech Stack:** Svelte 5.59.1, TypeScript 6.0.3, Vite 8.3.8, Vitest 5.1.1 with jsdom 31.0.1,
svelte-check 4.7.8, uPlot 1.6.32, `@playwright/test` 1.65.1 (Chromium), all as M16a left them.
Rust 1.100.0, no new crate. No new package anywhere.

**Spec:**
- `doc/research/Admin_Console.md` §4 (the Cluster row: members and their state, leases with
  holder, ring owner and epoch, the pipelines away from their owner, a node's page), §4.1 (a
  pending removal is yellow), §5 (node leave and shutdown, admin; a second confirmation where an
  action can't be undone), §7 (the console's status shows Prometheus failing), §8 (now first,
  age shown, the 300 ms budget).
- The approved mockups `doc/v1.1/mockups/03-cluster.html`, `04-node.html` and
  `11-node-actions-and-degraded-states.html` (the node dialogs, the console status).
- `doc/specs/System-Overview.md`, Management API endpoints (`/v1/cluster/status`,
  `/v1/nodes/{node}`, `/v1/nodes/{node}/leave|shutdown`, `/v1/history`, `/v1/events`,
  `/v1/stream`): **spec patch S-M16b.1** (below, approved and applied 2026-11-26) adds the new
  route and the status's new fields.
- `doc/specs/Configuration_Specification.md` §12.3 (removal tombstones: `removals/<id>.json`, the
  owner or the ring's node tears a removal down and marks it done).
- `doc/v1.1/M16a-console-pipelines.md` (the model, the chart, the dialog, the notices, the
  browser flows), `M13b-console-nodes.md` (`/v1/nodes/{node}`, the forwarded node actions),
  `M13e-console-history.md` (a node's series, `prometheus` in the answer),
  `M13f-console-stream.md` (`hello`, `node`).

### Spec patch S-M16b.1 (approved and applied 2026-11-26)

`doc/specs/System-Overview.md`, the Management API endpoints table. The Cluster status row's
semantics become:

> each member's state, API URL, config file hash and start (`started_at`); each pipeline's lease
> holder, its ring owner and when the lease expires; the cluster's settings (`tokens`,
> `lock_backend`, the lease duration, `rebalance_stabilization`, `max_concurrent_handoffs`,
> `drain_timeout`, and the console's history and events retention)

and a row follows it:

> | Pending removals | `GET /v1/cluster/removals` | viewer | the removal tombstones not done yet (Configuration §12.3), oldest first: each pipeline's id, the generation that removed it and when; `503` when they can't be read |

## Global Constraints

- Toolchain `1.100.0`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`;
  - in `ui/`: `npm run check` (svelte-check, warnings fail it), `npm test`, `npm run build`.
- **Node actions are an admin's** (Admin_Console §5; System-Overview: `admin`). An operator or a
  viewer sees none; the server checks the role again and the node acted on records it. Every
  call that changes something goes through M15's client (`X-CSRF-Token`).
- **Every action opens one dialog** (§5): what it does, to what, its effect; an optional note
  (at most 500 characters); the line it will be recorded as. A shutdown can't be undone from the
  console, so it asks for a second confirmation: the node's name typed (mockup 11).
- **Now first, history second; age always shown** (§8): a page draws what the console knows at
  once; each part is read on its own and keeps what it had when a read fails, saying which and
  how old; while the stream is stale every panel says how old it is; while offline actions wait.
- **Page budgets** (§8), with 50 pipelines on three nodes: the first page within 1 s, moving
  between pages within **300 ms**, the Overview to the Cluster and the Cluster to a node among
  the moves (Task 7).
- **Under the console's Content-Security-Policy**: no inline script, no `style` attribute.
- **No two files whose names differ only by case** (macOS): a component's pure logic lives in a
  file of another name (`cluster/derive.ts` beside `Cluster.svelte`, `nodes/view.ts` beside
  `Node.svelte`).
- **A Svelte effect reads only what it follows**: a call that reads and writes state of its own
  (`Renewals.load`) runs under `untrack`, or the effect runs again at each answer, for ever.
- **Test gates:** plain `cargo test` and `npm test` stay hermetic. The browser flows need
  `DATA_AVALANCHE_IT=1`, `DATA_AVALANCHE_BROWSER=1`, the app built into the binary and
  Playwright's Chromium; they run in the E2E suite (`--features e2e-hooks`) with
  `--test-threads=1`.
- **The site follows the behaviour in the same milestone** (`doc/v1.0/README.md`).
- **Code blocks:**
  - They are the verified content, already formatted (`rustfmt`; the TypeScript and Svelte as
    written, checked by svelte-check).
  - Every block is a `git apply`-ready diff, one per file.
  - A step's diffs apply in the order given, after the earlier steps'.
  - The regenerated files are written by their commands, not by hand:
    `crates/data-avalanche-api/openapi.json` by
    `DATA_AVALANCHE_BLESS=1 cargo test -p data-avalanche-api --lib openapi`, and
    `docs/public/openapi.json` and `docs/src/content/docs/reference/api.md` by
    `cargo xtask docs` (Task 1).
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to hurt someone running 1.1.0 in production. Each is pinned by a
test in the task after the arrow.

1. **A shutdown asked of the wrong node**, or of a node by a slip of the hand: nothing is sent
   until the node's own name is typed, exactly (trimmed, case kept) → Task 5 (`is recorded under
   the name of who asked, and needs the name typed to shut down`, `shuts down only once its name
   is typed`), Task 7 (`n2 shuts down only once its name is typed`).
2. **A leave or shutdown of the node the console's live updates come through**: the person must
   know the page will lose its stream and reconnect elsewhere → Task 5 (`says so when the node is
   the one the console's live updates come through`).
3. **A removal whose teardown never ends** (its owner down, a lease never freed): it must show,
   yellow past 2 min, and on the Cluster page → Task 2 (`says when a removal has waited past its
   threshold for its teardown, and links the cluster`), Task 3 (`lists the removals waiting for
   their teardown, oldest first, past the threshold in orange`, `lists the pending removals, with
   what the rule waits for`), Task 1 (`the_pending_removals_are_listed_until_they_are_done`).
4. **The cluster's read failing while a pipeline's page is open**: its lease must not count down
   from a stale end into a false "expired", and the page must say the cluster couldn't be read →
   Task 2 (`counts nothing down once the cluster's read fails: its lease's end is only what the
   last read saw`, `says when the cluster's leases couldn't be read, and counts its lease down no
   more`).
5. **A node gone, or out of the stream's reach**: a link to a node no longer a member says so; a
   member the stream can't follow says for how long; a part of its page that can't be read keeps
   what it had → Task 4 (`says a node the console's stream can't reach is unreachable, and a name
   no member has is none`, `says when no member has its name, and keeps what a failed part had`,
   `says which part couldn't be read and how old it is`).

## Decisions and rulings

Settled with Oleksii before this plan (2026-11-25): M16b is the Cluster and a node's page, node
actions, DA-API-21 (a route for pending removals, then the yellow rule) and the console's status;
it carries M16a's parked minors (the site's example notice, the lease header after a failed
cluster read) and the Overview's notice worded by `lib/notices.ts`. The rest are Claude's calls.

- **D-M16b.1 The server's part** (Task 1). The Cluster page's tiles and texts need what the
  config says of the cluster, and a member's age: `GET /v1/cluster/status` adds
  `members[].started_at` (the member's gossip incarnation, which is its start in ms) and
  `settings` (`tokens`, `lock_backend`, `lease_seconds`, `rebalance_stabilization_seconds`,
  `max_concurrent_handoffs`, `drain_timeout_seconds`, `history_retention_seconds`,
  `events_retention_seconds`). DA-API-21 takes a route of its own rather than a status field:
  the tombstones are read from storage, which the status (memory only) never waits on.
  `GET /v1/cluster/removals` (viewer) answers `{"removals": [{id, generation, removed_at}]}`
  from `Removals::all_pending()`, oldest first, or `503` "the removals can't be read: …". `Node`
  gains `removals` (the `Removals` the binary already builds).
- **D-M16b.2 The model** (Task 2). A `removals` part, read again on `config.*` and
  `pipeline.removed` events. The stream's `node` messages kept per member, the newest by `at`; a
  cluster read drops a node no longer a member. `Live.via`: the node of the last `hello`.
  `PART_NAMES` names each part as people read it ("the snapshot jobs", "the pending removals"),
  and the Overview, the list and a pipeline's page word their failing notice with
  `failingNotice(failures(…))`, the server's error in brackets (M16a's parked minor: the site's
  example said `HTTP 503`). A pipeline's page takes the cluster's read among its parts: while it
  fails the lease line counts nothing down (M16a's other parked minor). The rule: a removal
  pending over `REMOVAL_PENDING_S` (120 s, past one 30 s round of every node's tombstone check
  and a teardown) is yellow, "Removal of payments pending", linking the Cluster.
- **D-M16b.3 The Cluster page** (Task 3, mockup 03). Tiles: Members (`3 of 3`, the suspect and
  leaving named), Leases (those with no holder), Config generation (the nodes on another),
  Away from owner (the hour's last hand-off), Cluster (`128 tokens`, lease and lock backend).
  Members, each linking its page: state (`alive`, `suspect`, `leaving` for draining, `joining`;
  `· unreachable 40 s` while the stream can't follow it), address, version, generation (orange
  when another), pipelines held, its slowest lease renewal over the last hour, up. The
  pipelines away from their owner, while any: holder, owner, since, why (the hour's
  `lease.lost`/`lease.takeover` text) and when it goes back ("once n1 has been in the ring 15 s,
  1 hand-off at a time": `rebalance_stabilization`, `max_concurrent_handoffs`); red with no
  hand-off in the hour, or away over an hour. Every lease: no holder or not renewing first, then
  away, then the soonest to end. The pending removals, while any. The renewals: a member's
  `lease_renew_seconds` over the last hour from `GET /v1/history?node=…` (`Renewals`), read each
  time the page opens and when the members change. A value under a second is written in ms
  (`formatValue`). The tiles move to `lib/Tiles.svelte`, shared with the Overview.
- **D-M16b.4 A node's page** (Task 4, mockup 04, `/ui/cluster/nodes/{name}`). Header: state chip,
  `10.0.1.12:8080 · gossip 10.0.1.12:7946 · 1.1.0 · config gen 42 · started 3 d ago`, from its
  own detail (`GET /v1/nodes/{name}`), else the cluster's status. Its pipelines (ordered as the
  Overview's rows) with lag sparklines and leases. Three charts over 6 hours at a minute a step
  (`GET /v1/history?node=…&series=lease_renew_seconds,memory_bytes,cpu_cores`): lease renewal
  (its peak), memory (against its limit), CPU (against its cores). Its activity: the events it
  recorded (`GET /v1/events?node=…&limit=50`, then the stream's). Its gossip: the peers it sees,
  its last probe, the suspicions of the last day. A name no member has: "There is no member n9 in
  this node's view of the cluster."
- **D-M16b.5 The node actions** (Task 5, mockup 11). Leave and Shut down, an admin's, disabled
  offline or once the member isn't `active`; a line under them says there is no in-place
  restart. The dialogs name the pipelines the node holds ("the ring places each on another
  node": the target is the ring's, not the console's), a running snapshot going on from its last
  chunk, the hand-offs one at a time and `drain_timeout`, and for a shutdown who carries the
  pipelines meanwhile and that only a supervisor starts it again. Recorded as
  `n2 left the cluster, asked by Oleksii (admin)` and `n2 shut down by Oleksii (admin)`. When the
  console's stream comes through that node, the dialog says the page will reconnect elsewhere.
  `POST /v1/nodes/{name}/leave|shutdown` with the note; what follows (the event, the member's
  phase) comes on the stream.
- **D-M16b.6 The console's status** (Task 6, mockup 11). The user menu (the name and role, a
  `<details>`) shows a table, drawn only while open: Live updates (`connected via n1`,
  `reconnecting`/`offline` with the last update's age), Nodes answering (`2 of 3 · n3 40 s`),
  Built-in history (`24 h · last sample 6 s ago`), Prometheus (`not configured`, `ok`, `failing`
  with its error, and when the node last asked it), Event store (`ok · 30 days`, or `failing`
  with why). Prometheus's state is the `prometheus` field of the lag lines' `/v1/history` answer;
  opening the menu reads the lag lines and the events again.
- **D-M16b.7 Where the mockups and the server differ** (rulings):
  - *"Renews in 8 s"* (mockup 03): the status gives when a lease ends unless renewed; the
    leases say `in 8 s` under **Expires** (as M16a's header).
  - *"Lease renewal p99"* (mockups 03, 04): the history keeps each step's slowest renewal, not a
    percentile; the column is "Lease renewal, slowest 1 h".
  - *A per-pipeline target in the leave dialog* (mockup 11): the ring places each pipeline when
    the node leaves; the console doesn't know where until it happens, so the dialog says so.
  - *The pod name and the incarnation* (mockup 04's header): the API has neither as such; the
    header shows the API and gossip addresses and the start.
  - *"failing since 14:20"* (mockup 11, Prometheus): the node keeps its last read's outcome only,
    so the status says when the node last asked it.
- **D-M16b.8 The navigation budget** (Task 7). Two more moves on the budget's cluster: the
  Overview to the Cluster (its 50 lease rows) and the Cluster to a node (its pipelines' rows),
  each under 300 ms.
- **D-M16b.9 Rollback.** Nothing is stored. `GET /v1/cluster/status` only gains fields; the new
  route only reads.

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-api/src/{cluster,lib,test_support}.rs`, `crates/data-avalanche/src/run.rs`, `openapi.json`, `docs/public/openapi.json`, `docs/…/reference/api.md` | The status's new fields, the removals route (Task 1) |
| `ui/src/model/{types,facts,fixtures,model.svelte}.ts`, `ui/src/lib/live.svelte.ts`, `ui/src/overview/{attention.ts,Overview.svelte}`, `ui/src/pipelines/{detail.ts,List.svelte,Pipeline.svelte}` | The model's new parts, the removal rule, one notice wording (Task 2) |
| `ui/src/cluster/{derive.ts,renewals.svelte.ts,Cluster.svelte}` (new), `ui/src/lib/{Tiles.svelte (new),series.ts}`, `ui/src/overview/Overview.svelte`, `ui/src/App.svelte` | The Cluster page (Task 3) |
| `ui/src/nodes/{view.ts,page.svelte.ts,Node.svelte,fixtures.ts}` (new), `ui/src/cluster/derive.ts`, `ui/src/App.svelte` | A node's page (Task 4) |
| `ui/src/nodes/actions.ts` (new), `ui/src/nodes/Node.svelte`, `ui/src/App.svelte` | The node actions (Task 5) |
| `ui/src/lib/status.ts` (new), `ui/src/Frame.svelte`, `ui/src/model/*`, `ui/src/App.svelte` | The console's status (Task 6) |
| `ui/e2e/{cluster.spec.ts (new),budget.spec.ts}`, `crates/data-avalanche/tests/e2e/console_app.rs`, `docs/…/deployment/console.md` | In a browser; the site (Task 7) |

The plan's commit also carries `doc/v1.1/README.md` (M16a merged, #35; this plan's row). Two
more commits go with it: `research: Admin_Console — §4.1 the removal rule; §4.4 the Cluster as
M16b builds it` and `gap: v1 §71 — 1.1.0 M16b's plan, DA-API-21 planned`.

---

### Task 1: The cluster's settings and the pending removals (`/v1/cluster/status`, `/v1/cluster/removals`)

**Files:**
- Regenerate: `crates/data-avalanche-api/openapi.json`
- Modify: `crates/data-avalanche-api/src/cluster.rs`
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Modify: `crates/data-avalanche-api/src/test_support.rs`
- Modify: `crates/data-avalanche/src/run.rs`
- Regenerate: `docs/public/openapi.json`
- Regenerate: `docs/src/content/docs/reference/api.md`

**Interfaces:**
- Consumes: `Removals::all_pending()` (`data-avalanche-state`, M8b: each tombstone's
  `pipeline_id`, `generation`, `removed_at`); the binary's `deps.removals`.
- Produces (D-M16b.1): `GET /v1/cluster/status`'s `members[].started_at` (RFC 3339, `null` when
  the incarnation isn't a time) and `settings` (`tokens`, `lock_backend`, `lease_seconds`,
  `rebalance_stabilization_seconds`, `max_concurrent_handoffs`, `drain_timeout_seconds`,
  `history_retention_seconds`, `events_retention_seconds`); `GET /v1/cluster/removals` (viewer):
  `{"removals": [{"id", "generation", "removed_at"}]}`, oldest first; `Node.removals: Removals`;
  `test_support::Lone.removals`.

- [ ] **Step 1: Write the failing tests**

The status test expects the new fields (the test config's settings; the harness's
incarnation is 1, so `started_at` is `1970-01-01T00:00:00.001Z`); a new test records two
tombstones, lists them, marks one done and lists the other; the RBAC table names the route.

`crates/data-avalanche-api/src/cluster.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/cluster.rs b/crates/data-avalanche-api/src/cluster.rs
index ca2a2dc78e5b1c813beffbca713ae208aa56f2a6..3d61d85a711f0895fd6b468dd81e71ce3ab9e9f6 100644
--- a/crates/data-avalanche-api/src/cluster.rs
+++ b/crates/data-avalanche-api/src/cluster.rs
@@ -211,6 +211,8 @@ mod tests {
         assert_eq!(v["members"][0]["phase"], "active");
         assert_eq!(v["members"][0]["config_generation"], 1);
         assert_eq!(v["members"][0]["api_url"], "http://127.0.0.1:8080");
+        // The test's gossip gives its node incarnation 1: 1 ms past the epoch.
+        assert_eq!(v["members"][0]["started_at"], "1970-01-01T00:00:00.001Z");
         assert_eq!(
             v["members"][0]["file_hash"],
             data_avalanche_cluster::file_hash(
@@ -225,6 +227,16 @@ mod tests {
                 "expires_at": null, "ring_owner": "node-a",
             }])
         );
+        assert_eq!(
+            v["settings"],
+            json!({
+                "tokens": 128, "lock_backend": "storage", "lease_seconds": 30,
+                "rebalance_stabilization_seconds": 15, "max_concurrent_handoffs": 2,
+                "drain_timeout_seconds": 45, "history_retention_seconds": 86_400,
+                "events_retention_seconds": 2_592_000,
+            }),
+            "the cluster's settings the console states"
+        );
         let holder = Holder {
             node_id: "node-b".into(),
             incarnation: 1,
@@ -253,6 +265,55 @@ mod tests {
         );
     }
 
+    /// DA-API-21: the removals not done yet, oldest first; a done one is gone from the list.
+    #[tokio::test]
+    async fn the_pending_removals_are_listed_until_they_are_done() {
+        let dir = tempfile::tempdir().unwrap();
+        let file = dir.path().join("config.yaml");
+        std::fs::write(&file, CONFIG).unwrap();
+        let lone = lone(file).await;
+        let a = api(open());
+        a.node.set(lone.node.clone()).unwrap();
+        let addr = serve(a).await;
+        assert_eq!(
+            json_of(addr, "GET", "/v1/cluster/removals").await,
+            (200, json!({ "removals": [] }))
+        );
+
+        let at = |s: &str| s.parse::<chrono::DateTime<Utc>>().unwrap();
+        for (id, removed_at) in [
+            ("payments", "2026-11-26T10:05:00Z"),
+            ("billing", "2026-11-26T10:00:00Z"),
+        ] {
+            let doc = data_avalanche_state::RemovalDoc {
+                pipeline_id: id.into(),
+                generation: 3,
+                hash: "blake3:00".into(),
+                removed_at: at(removed_at),
+                done_at: None,
+                records: 0,
+            };
+            lone.removals.record(&doc).await.unwrap();
+        }
+        assert_eq!(
+            json_of(addr, "GET", "/v1/cluster/removals").await,
+            (
+                200,
+                json!({ "removals": [
+                    { "id": "billing", "generation": 3, "removed_at": "2026-11-26T10:00:00Z" },
+                    { "id": "payments", "generation": 3, "removed_at": "2026-11-26T10:05:00Z" },
+                ] })
+            )
+        );
+        lone.removals
+            .done("billing", at("2026-11-26T10:06:00Z"))
+            .await
+            .unwrap();
+        let v = json_of(addr, "GET", "/v1/cluster/removals").await.1;
+        assert_eq!(v["removals"].as_array().map(Vec::len), Some(1), "{v}");
+        assert_eq!(v["removals"][0]["id"], "payments");
+    }
+
     #[tokio::test]
     async fn a_leave_and_a_shutdown_are_asked_of_the_binary() {
         let dir = tempfile::tempdir().unwrap();
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 11e538e489289996e4127e7243a2d4f04569e89d..9f4bb7a6477205731d7bce419d7ce7b1e61e8a69 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -1067,6 +1067,7 @@ mod tests {
                 ("GET", "/v1/config/generations", Viewer),
                 ("GET", "/v1/config/generations/{generation}", Viewer),
                 ("GET", "/v1/cluster/status", Viewer),
+                ("GET", "/v1/cluster/removals", Viewer),
                 ("GET", "/v1/pipelines", Viewer),
                 ("GET", "/v1/pipelines/{id}", Viewer),
                 ("GET", "/v1/pipelines/{id}/live", Viewer),
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index 4c5b529c1e538f3a83621a562e0ebaf36091b4d0..4cc1042833eaee139ddd53500f9953869b8940e3 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -243,6 +243,9 @@ pub(crate) struct Lone {
     pub events: Arc<Events>,
     /// The leases the node reads: a test takes them as another node would.
     pub leases: Leases,
+    /// The removal tombstones the node's coordinator records: a test records them as a reload
+    /// would.
+    pub removals: Removals,
     /// What the API asked of the node.
     pub stops: tokio::sync::watch::Receiver<Stop>,
     pub _reconciler: ClusterReconciler,
@@ -344,7 +347,7 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
         first.pointer,
         CoordinatorDeps {
             generations,
-            removals,
+            removals: removals.clone(),
             env,
             key,
             node_id: "node-a".into(),
@@ -391,6 +394,7 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
         },
         events,
         leases,
+        removals,
         stops,
         _reconciler: reconciler,
         _gossip: gossip,
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api --lib`
Expected: 3 failed, 237 passed:
`the_cluster_status_names_the_members_and_the_holders` (`started_at`: `left: Null`),
`the_pending_removals_are_listed_until_they_are_done` (the route isn't there: the answer is the
console's page, `expected value at line 1 column 1`), `the_routes_take_the_roles_of_api_rbac`
(the table lacks `GET /v1/cluster/removals`).

- [ ] **Step 3: Implement**

`status` writes the new fields; `removals` answers from `node.removals`; the route goes
after `/v1/cluster/status` (Viewer); `Node` gains `removals`, which `run.rs` passes. Then
regenerate the API's documents: `DATA_AVALANCHE_BLESS=1 cargo test -p data-avalanche-api --lib
openapi` and `cargo xtask docs`.

`crates/data-avalanche-api/src/cluster.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/cluster.rs b/crates/data-avalanche-api/src/cluster.rs
index 3d61d85a711f0895fd6b468dd81e71ce3ab9e9f6..73d719eb7d6e3672b5bb3d52174a02125b7a59dd 100644
--- a/crates/data-avalanche-api/src/cluster.rs
+++ b/crates/data-avalanche-api/src/cluster.rs
@@ -81,6 +81,10 @@ pub(crate) async fn status(State(api): State<Api>) -> Response {
                 "build": m.meta.build,
                 "api_url": (!m.meta.api_url.is_empty()).then_some(&m.meta.api_url),
                 "file_hash": (!m.meta.file_hash.is_empty()).then_some(&m.meta.file_hash),
+                // A member's incarnation is when its process started (ms since the epoch).
+                "started_at": i64::try_from(m.incarnation)
+                    .ok()
+                    .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis),
             })
         })
         .collect();
@@ -121,12 +125,59 @@ pub(crate) async fn status(State(api): State<Api>) -> Response {
             "ring_owner": ring.owner(id),
         }));
     }
+    let c = &config.globals.cluster;
+    let secs = |d: std::time::Duration| d.as_secs();
+    let settings = json!({
+        "tokens": c.tokens,
+        "lock_backend": c.lock_backend,
+        "lease_seconds": secs(c.lease_duration),
+        "rebalance_stabilization_seconds": secs(c.rebalance_stabilization),
+        "max_concurrent_handoffs": c.max_concurrent_handoffs,
+        "drain_timeout_seconds": secs(c.drain_timeout),
+        "history_retention_seconds": secs(config.globals.console.history.retention),
+        "events_retention_seconds": secs(config.globals.console.events.retention),
+    });
     body(
         StatusCode::OK,
-        &json!({ "node": members.me, "members": nodes, "pipelines": pipelines }),
+        &json!({
+            "node": members.me,
+            "members": nodes,
+            "pipelines": pipelines,
+            "settings": settings,
+        }),
     )
 }
 
+/// The removals not torn down yet (DA-API-21; Configuration §12.3), oldest first: a pipeline
+/// whose tombstone waits for its owner, or for the node the ring places it on, to tear it down.
+pub(crate) async fn removals(State(api): State<Api>) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    match node.removals.all_pending().await {
+        Ok(mut pending) => {
+            pending.sort_by(|a, b| {
+                (a.removed_at, &a.pipeline_id).cmp(&(b.removed_at, &b.pipeline_id))
+            });
+            let removals: Vec<Value> = pending
+                .iter()
+                .map(|r| {
+                    json!({
+                        "id": r.pipeline_id,
+                        "generation": r.generation,
+                        "removed_at": r.removed_at,
+                    })
+                })
+                .collect();
+            body(StatusCode::OK, &json!({ "removals": removals }))
+        }
+        Err(e) => body(
+            StatusCode::SERVICE_UNAVAILABLE,
+            &json!({ "error": format!("the removals can't be read: {e}") }),
+        ),
+    }
+}
+
 /// 202 once the leave is asked, naming the node that leaves; 409 when this node leaves, or
 /// shuts down, already.
 pub(crate) async fn leave(State(api): State<Api>, Extension(by): Extension<Principal>) -> Response {
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 9f4bb7a6477205731d7bce419d7ce7b1e61e8a69..3d93d5bca94417fa47ad0930e0eaee68a4350f40 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -54,7 +54,7 @@ use data_avalanche_cluster::{
     WorkerFactory,
 };
 use data_avalanche_config::Role;
-use data_avalanche_state::{Charts, Events, Leases};
+use data_avalanche_state::{Charts, Events, Leases, Removals};
 use data_avalanche_telemetry::{CONTENT_TYPE, Metrics, ProcessFigures};
 use serde_json::{Value, json};
 use tokio::sync::watch;
@@ -115,6 +115,9 @@ pub struct Node {
     pub docs: Arc<DocsFn>,
     /// The leases in `globals.state`: who holds each pipeline.
     pub leases: Leases,
+    /// The removal tombstones (Configuration §12.3), the pending ones read for the console
+    /// (DA-API-21).
+    pub removals: Removals,
     /// The node's leave and shutdown.
     pub control: Control,
     /// The posted-body validates' limit ([`RateLimit::validate`], DA-API-20).
@@ -362,11 +365,21 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
                 "The members this node sees, and each pipeline's lease holder",
                 &[(
                     200,
-                    "the members and the holders; a member's `file_hash` is its file's keyed fingerprint (first 16 hex), comparable across nodes of one cluster only",
+                    "the members and the holders, and the cluster's settings a console states (`settings`: its tokens, lock, lease and hand-off settings and the history's and the events' retention); a member's `file_hash` is its file's keyed fingerprint (first 16 hex), comparable across nodes of one cluster only",
                 )],
             ),
             get(cluster::status),
         ),
+        (
+            r(
+                "GET",
+                "/v1/cluster/removals",
+                Viewer,
+                "The pipeline removals not torn down yet, oldest first: each one's id, the last generation that had it, and when it was removed",
+                &[(200, "the pending removals")],
+            ),
+            get(cluster::removals),
+        ),
         (
             r(
                 "GET",
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index 4cc1042833eaee139ddd53500f9953869b8940e3..c9155f8f265beb82d85d2ee39232d013351a949d 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -376,6 +376,7 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
                 Ok(PipelineDocs::new(DocStore::new(Arc::new(store))))
             }),
             leases: leases.clone(),
+            removals: removals.clone(),
             control,
             validates: Arc::new(crate::RateLimit::validate()),
             recorder,
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 3a5c27a89ed020253f78cdbd1f6f8e05d16b33ba..e9af2c8cf3f7fd3c61e1ae14d9d37e995fa81513 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -323,6 +323,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         file: args.config.clone(),
         docs,
         leases: deps.leases.clone(),
+        removals: deps.removals.clone(),
         control,
         validates: Arc::new(RateLimit::validate()),
         recorder: recorder.clone(),
````

- [ ] **Step 4: Run the tests and the gates**

Run `cargo test -p data-avalanche-api --lib`: 240 passed. Then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
(cd ui && npm run check && npm test && npm run build)
```

and `cargo xtask docs --check`.

Expected: no output from the first three; every test passes; svelte-check reports 0 errors and
0 warnings.

- [ ] **Step 5: Commit**

```bash
git add -A crates/data-avalanche-api/openapi.json crates/data-avalanche-api/src/cluster.rs crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/test_support.rs crates/data-avalanche/src/run.rs docs/public/openapi.json docs/src/content/docs/reference/api.md
git commit -m "feat(api): the cluster status names each member's start and the cluster's settings; GET /v1/cluster/removals lists the pending removals"
```

---

### Task 2: The model's removals, nodes and stream; the removal rule; one notice wording (`ui/src/model`, the parked minors)

**Files:**
- Modify: `ui/src/lib/live.svelte.ts`
- Modify: `ui/src/lib/live.test.ts`
- Modify: `ui/src/model/facts.ts`
- Modify: `ui/src/model/fixtures.ts`
- Modify: `ui/src/model/model.svelte.ts`
- Modify: `ui/src/model/model.test.ts`
- Modify: `ui/src/model/types.ts`
- Modify: `ui/src/overview/Overview.svelte`
- Modify: `ui/src/overview/Overview.test.ts`
- Modify: `ui/src/overview/attention.test.ts`
- Modify: `ui/src/overview/attention.ts`
- Modify: `ui/src/pipelines/List.svelte`
- Modify: `ui/src/pipelines/Pipeline.svelte`
- Modify: `ui/src/pipelines/Pipeline.test.ts`
- Modify: `ui/src/pipelines/detail.test.ts`
- Modify: `ui/src/pipelines/detail.ts`

**Interfaces:**
- Consumes: Task 1's fields and route; M16a's `Model`, `lib/notices.ts` (`failures`,
  `failingNotice`, `PartRead`), `pipelines/detail.ts`'s `header`.
- Produces (D-M16b.2): `types.ts`'s `Member.started_at`/`.api_url`/`.build`, `ClusterSettings`,
  `ClusterStatus.settings?`, `Removal { id; generation; removed_at }`, `Process`, `Gossip`,
  `NodeFigures`; `Facts.removals: Removal[]`, `Facts.nodes: Record<string, NodeFigures>`;
  `Part` gains `'removals'`; `PART_NAMES: Record<Part, string>`; `Model.node(m: NodeFigures)`;
  `Live.via: string | null`; `fixtures.ts`'s `nodeFigures(node, over?)`, members with addresses,
  builds and starts, `settings`; `attention.ts`'s `REMOVAL_PENDING_S = 120`;
  `header(d, item, fig, lease, now, clusterRead: PartRead = { at: null, error: null })`.

- [ ] **Step 1: Write the failing tests**

The model reads `/v1/cluster/removals` and names its parts; a `node` message is kept,
the newest by `at`, and a member gone drops its figures; `Live` names the node of each `hello`;
the removal rule; the Overview's notice in `lib/notices.ts`'s words; a pipeline's header and
page after a failed cluster read.

`ui/src/lib/live.test.ts`:

````diff
diff --git a/ui/src/lib/live.test.ts b/ui/src/lib/live.test.ts
index 9ca3df443603592189f2807d1b5e9915781d319d..1549e9b2cb86b59060b024d4279d450c1c176c6f 100644
--- a/ui/src/lib/live.test.ts
+++ b/ui/src/lib/live.test.ts
@@ -56,6 +56,17 @@ describe('the live stream', () => {
     expect(live.last).toBe(NOW);
   });
 
+  it('names the node it comes through, from each hello, and none before the first', () => {
+    const live = new Live({ open, check: vi.fn() });
+    live.start();
+    expect(live.via).toBeNull();
+    sources[0].send('hello', { node: 'n1', follows: ['n2'] });
+    expect(live.via).toBe('n1');
+    // The browser reconnected by itself, to another node behind the address.
+    sources[0].send('hello', { node: 'n3' });
+    expect(live.via).toBe('n3');
+  });
+
   it('turns stale, then offline, as the messages stop, and reopens a silent stream', async () => {
     const live = new Live({ open, check: vi.fn().mockResolvedValue('ok') });
     live.start();
````

`ui/src/model/facts.ts`:

````diff
diff --git a/ui/src/model/facts.ts b/ui/src/model/facts.ts
index 06592333f114f11b0b531b43b4615d8eac7568f2..bb4e0239488561b59208e11c2ac32679feed7023 100644
--- a/ui/src/model/facts.ts
+++ b/ui/src/model/facts.ts
@@ -7,7 +7,9 @@ import type {
   EventItem,
   Figures,
   Job,
+  NodeFigures,
   PipelineItem,
+  Removal,
   SinkAt,
   SourceAt,
   TableItem,
@@ -34,6 +36,10 @@ export interface Facts {
   unreachable: Record<string, number>;
   /** The pipelines whose lease no node holds (or renews), since when the page saw it (ms). */
   unheld: Record<string, number>;
+  /** The removals not torn down yet, oldest first (DA-API-21). */
+  removals: Removal[];
+  /** Each member's own figures, as the stream last sent them. */
+  nodes: Record<string, NodeFigures>;
 }
 
 export function empty(): Facts {
@@ -50,6 +56,8 @@ export function empty(): Facts {
     leaseLosses: [],
     unreachable: {},
     unheld: {},
+    removals: [],
+    nodes: {},
   };
 }
 
````

`ui/src/model/fixtures.ts`:

````diff
diff --git a/ui/src/model/fixtures.ts b/ui/src/model/fixtures.ts
index 2408e0ca8845c730c323faee9eedeaa39c47e586..d7cc4f54705f3629ca68f22f13b36b6640c6f79f 100644
--- a/ui/src/model/fixtures.ts
+++ b/ui/src/model/fixtures.ts
@@ -1,7 +1,7 @@
 // The tests' cluster: mockup 01's, in the API's shapes.
 
 import type { Facts } from './facts';
-import type { Actual, ClusterStatus, Desired, EventItem, Figures, PipelineItem, SourceAt, TableItem } from './types';
+import type { Actual, ClusterStatus, Desired, EventItem, Figures, NodeFigures, PipelineItem, SourceAt, TableItem } from './types';
 
 export const NOW = Date.UTC(2026, 10, 25, 14, 2, 0);
 const MIN = 60_000;
@@ -57,6 +57,24 @@ function lease(id: string, holder: string, owner_epoch: number, ring_owner: stri
   return { id, holder, holder_stale: false, owner_epoch, expires_at: iso(NOW + 8_000), ring_owner };
 }
 
+/** A node's live figures: alive peers, a recent probe, 1.3 GiB of a 2 GiB limit, and `pipelines`. */
+export function nodeFigures(node: string, over: Partial<NodeFigures> = {}): NodeFigures {
+  return {
+    node,
+    at: iso(NOW - 2_000),
+    phase: 'active',
+    process: { memory_bytes: 1.3 * GIB, memory_limit_bytes: 2 * GIB, cpu_seconds: 5400, cpu_cores: 0.6, cpu_limit_cores: 2 },
+    gossip: {
+      peers: ['n1', 'n2', 'n3'].filter((n) => n !== node).map((n) => ({ node: n, state: 'alive' as const, phase: 'active' })),
+      last_probe: { node: node === 'n1' ? 'n2' : 'n1', at: iso(NOW - 400) },
+      probe_rtt_ms: 1.2,
+      suspicions: { raised: [], received: 0, raised_more: 0, received_more: 0 },
+    },
+    pipelines: [],
+    ...over,
+  };
+}
+
 /** A table with nothing to say but what `over` says: streaming, no figures yet. */
 export function table(pipeline: string, name: string, over: Partial<TableItem> = {}): TableItem {
   return {
@@ -104,7 +122,15 @@ export function facts(): Facts {
     ],
     cluster: {
       node: 'n1',
-      members: ['n1', 'n2', 'n3'].map((node) => ({ node, phase: 'active', suspect: false, config_generation: 42 })),
+      members: ['n1', 'n2', 'n3'].map((node, i) => ({
+        node,
+        addr: `10.0.1.1${i + 1}:7946`,
+        phase: 'active',
+        suspect: false,
+        config_generation: 42,
+        build: '1.1.0',
+        api_url: `https://10.0.1.1${i + 1}:8080`,
+      })),
       pipelines: [
         // Held by n2 away from its ring owner, since a failover.
         lease('orders-pg', 'n2', 2, 'n1'),
@@ -112,6 +138,16 @@ export function facts(): Facts {
         lease('inventory', 'n3', 1, 'n3'),
         lease('customers', 'n1', 1, 'n1'),
       ],
+      settings: {
+        tokens: 128,
+        lock_backend: 'storage',
+        lease_seconds: 15,
+        rebalance_stabilization_seconds: 15,
+        max_concurrent_handoffs: 1,
+        drain_timeout_seconds: 60,
+        history_retention_seconds: 86_400,
+        events_retention_seconds: 2_592_000,
+      },
     },
     config: {
       generation: 42,
@@ -171,5 +207,7 @@ export function facts(): Facts {
     leaseLosses: [],
     unreachable: {},
     unheld: {},
+    removals: [],
+    nodes: Object.fromEntries(['n1', 'n2', 'n3'].map((n) => [n, nodeFigures(n)])),
   };
 }
````

`ui/src/model/model.test.ts`:

````diff
diff --git a/ui/src/model/model.test.ts b/ui/src/model/model.test.ts
index 3ded598e429932e4460b2ded141a51c66fab6ff7..3aec9b7cab4a4a5ffbef43272c481005459a6b50 100644
--- a/ui/src/model/model.test.ts
+++ b/ui/src/model/model.test.ts
@@ -1,7 +1,7 @@
 import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
 import { HttpError, type Client } from '../lib/api';
-import { event, facts, figures, iso, NOW } from './fixtures';
-import { Model, REREAD_AFTER_MS } from './model.svelte';
+import { event, facts, figures, iso, nodeFigures, NOW } from './fixtures';
+import { Model, PART_NAMES, REREAD_AFTER_MS } from './model.svelte';
 
 /** A `Client` answering each path from `answers`; a path not there fails. */
 function api(answers: Record<string, unknown>): Client & { asked: string[] } {
@@ -38,6 +38,9 @@ const READS = {
   [`/v1/events?kind=lease.lost,lease.takeover&since=${iso(NOW - 3_600_000)}&limit=1000`]: {
     events: [event({ kind: 'lease.takeover', at: iso(NOW - 50 * 60_000), pipeline: 'customers', text: 'customers taken over from n1 by n3' })],
   },
+  '/v1/cluster/removals': {
+    removals: [{ id: 'payments', generation: 41, removed_at: iso(NOW - 4 * 60_000) }],
+  },
   '/v1/history?series=lag_seconds': {
     series: [{ name: 'lag_seconds', pipeline: 'orders-pg', points: [[NOW / 1000, 21]] }],
   },
@@ -57,6 +60,8 @@ describe("the console's model", () => {
     expect(o.facts.actions.customers.kind).toBe('action.pipeline.resume');
     expect(o.facts.leaseLosses.map((e) => e.text)).toEqual(['customers taken over from n1 by n3']);
     expect(o.facts.lag['orders-pg']).toEqual([[NOW / 1000, 21]]);
+    expect(o.facts.removals.map((r) => r.id)).toEqual(['payments']);
+    expect(o.facts.cluster?.settings?.lease_seconds).toBe(15);
     expect(Object.values(o.read).every((r) => r.at === NOW && r.error === null)).toBe(true);
     const { '/v1/snapshots': _, ...without } = READS;
     const later = new Model(api(without));
@@ -178,6 +183,42 @@ describe("the console's model", () => {
     expect(client.asked).toEqual([]);
     await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
     // A person's pause, stop or resume changes the pipeline's desired state: its row reads again.
-    expect(client.asked.sort()).toEqual(['/v1/config/nodes', '/v1/pipelines', '/v1/snapshots']);
+    expect(client.asked.sort()).toEqual(['/v1/cluster/removals', '/v1/config/nodes', '/v1/pipelines', '/v1/snapshots']);
+  });
+  it("keeps each member's own figures, the newest a node sent, and drops a node no longer a member", async () => {
+    const o = new Model(api(READS));
+    o.node(nodeFigures('n2', { at: iso(NOW - 5_000), pipelines: ['orders-pg'] }));
+    o.node(nodeFigures('n2', { at: iso(NOW - 10_000), pipelines: [] }));
+    expect(o.facts.nodes.n2.pipelines).toEqual(['orders-pg']);
+    o.node(nodeFigures('n4'));
+    await o.load(['cluster']);
+    expect(Object.keys(o.facts.nodes)).toEqual(['n2']);
+  });
+
+  it('reads the pending removals again when a pipeline is removed or a generation published', async () => {
+    const a = api(READS);
+    const o = new Model(a);
+    o.event(event({ kind: 'pipeline.removed', at: iso(NOW), pipeline: 'payments' }));
+    await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
+    expect(a.asked).toContain('/v1/cluster/removals');
+    a.asked.length = 0;
+    o.event(event({ kind: 'config.published', at: iso(NOW), text: 'generation 43 published by ops' }));
+    await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
+    expect(a.asked).toEqual(expect.arrayContaining(['/v1/config/nodes', '/v1/cluster/removals']));
+  });
+
+  it('names its parts as people read them, for the notices that say which failed', () => {
+    expect(PART_NAMES).toEqual({
+      pipelines: 'the pipelines',
+      cluster: 'the cluster',
+      config: "the nodes' config",
+      jobs: 'the snapshot jobs',
+      tables: 'the tables',
+      events: 'the events',
+      actions: "the pipelines' last actions",
+      leases: "the hour's lease hand-offs",
+      lag: 'the lag lines',
+      removals: 'the pending removals',
+    });
   });
 });
````

`ui/src/model/types.ts`:

````diff
diff --git a/ui/src/model/types.ts b/ui/src/model/types.ts
index eb2a07490af20feca7399ee950a3e8bc3f3ac283..18daac32b7c0c861044cd5eebc51bb92de86beda 100644
--- a/ui/src/model/types.ts
+++ b/ui/src/model/types.ts
@@ -31,10 +31,35 @@ export interface PipelineItem {
   sink: SinkAt;
 }
 
+/** A member as `GET /v1/cluster/status` lists it. */
+export interface Member {
+  node: string;
+  /** Its gossip address. */
+  addr?: string;
+  phase: string;
+  suspect: boolean;
+  config_generation: number | null;
+  build?: string;
+  api_url?: string | null;
+}
+
+/** The cluster's settings a console states (`GET /v1/cluster/status`'s `settings`). */
+export interface ClusterSettings {
+  tokens: number;
+  lock_backend: string;
+  lease_seconds: number;
+  rebalance_stabilization_seconds: number;
+  max_concurrent_handoffs: number;
+  drain_timeout_seconds: number;
+  history_retention_seconds: number;
+  events_retention_seconds: number;
+}
+
 /** `GET /v1/cluster/status`. */
 export interface ClusterStatus {
   node: string;
-  members: { node: string; phase: string; suspect: boolean; config_generation: number | null }[];
+  members: Member[];
+  settings?: ClusterSettings;
   pipelines: {
     id: string;
     holder: string | null;
@@ -205,3 +230,44 @@ export interface PipelineDetail {
   position: string | null;
   halted_tables: { table: string; code: string; detail: string; since: string | null }[];
 }
+
+/** A removal not torn down yet (`GET /v1/cluster/removals`, DA-API-21). */
+export interface Removal {
+  id: string;
+  /** The last config generation that had the pipeline. */
+  generation: number;
+  removed_at: string;
+}
+
+/** A node's process, as its detail and its live `node` message show it. */
+export interface Process {
+  memory_bytes: number | null;
+  memory_limit_bytes: number | null;
+  cpu_seconds: number | null;
+  cpu_cores: number | null;
+  cpu_limit_cores: number | null;
+}
+
+/** What a node's gossip saw: its peers, its last probe, the suspicions of the last day. */
+export interface Gossip {
+  peers: { node: string; state: 'alive' | 'suspect'; phase: string }[];
+  last_probe: { node: string; at: string } | null;
+  probe_rtt_ms: number | null;
+  suspicions: {
+    raised: { node: string; at: string }[];
+    received: number;
+    raised_more: number;
+    received_more: number;
+  };
+}
+
+/** The stream's `node` message: a node's own figures, sent every 5 s. */
+export interface NodeFigures {
+  node: string;
+  at: string;
+  phase: string | null;
+  process: Process;
+  gossip: Gossip;
+  /** The pipelines it runs. */
+  pipelines: string[];
+}
````

`ui/src/overview/Overview.test.ts`:

````diff
diff --git a/ui/src/overview/Overview.test.ts b/ui/src/overview/Overview.test.ts
index 442f7d3083369366a0ad575b1a09190bff7609b6..bb9f7e338c6cdf76a862016b87d32eac69f45f9d 100644
--- a/ui/src/overview/Overview.test.ts
+++ b/ui/src/overview/Overview.test.ts
@@ -77,7 +77,7 @@ describe('the Overview', () => {
     expect(text(page.querySelector('.stale-note'))).toMatch(/^Not current: as of \d\d:\d\d:\d\d · 34 s old$/);
     expect(page.querySelector('.tiles')?.classList.contains('stale')).toBe(true);
     expect(text(page.querySelector('.failing'))).toBe(
-      "Couldn't be read, shown as last read: jobs (the request timed out)",
+      "Couldn't be read; showing what was last read: the snapshot jobs (the request timed out), last read 1 min ago",
     );
   });
 
````

`ui/src/overview/attention.test.ts`:

````diff
diff --git a/ui/src/overview/attention.test.ts b/ui/src/overview/attention.test.ts
index 9201149fe1e708a23c910c76d4c977e898ca9c82..87598f78ced410f61f1dd3f3ae10220dc7639beb 100644
--- a/ui/src/overview/attention.test.ts
+++ b/ui/src/overview/attention.test.ts
@@ -1,5 +1,5 @@
 import { describe, expect, it } from 'vitest';
-import { attention, LAG_TARGET_S } from './attention';
+import { attention, LAG_TARGET_S, REMOVAL_PENDING_S } from './attention';
 import { event, facts, figures, iso, NOW } from '../model/fixtures';
 
 const MIN = 60_000;
@@ -153,4 +153,23 @@ describe('needs attention', () => {
     f.actions = {};
     expect(attention(f, NOW)).toEqual([]);
   });
+  it('says when a removal has waited past its threshold for its teardown, and links the cluster', () => {
+    const f = facts();
+    f.removals = [
+      { id: 'payments', generation: 41, removed_at: iso(NOW - REMOVAL_PENDING_S * 1000 - 1_000) },
+      { id: 'audit', generation: 42, removed_at: iso(NOW - 30_000) },
+    ];
+    const pending = attention(f, NOW).filter((a) => a.what.startsWith('Removal'));
+    expect(pending).toEqual([
+      {
+        severity: 'yellow',
+        what: 'Removal of payments pending',
+        why: 'its teardown hasn\'t finished since generation 41 removed it',
+        since: NOW - REMOVAL_PENDING_S * 1000 - 1_000,
+        href: '/ui/cluster',
+        link: 'Cluster',
+      },
+    ]);
+    expect(REMOVAL_PENDING_S).toBe(120);
+  });
 });
````

`ui/src/pipelines/Pipeline.test.ts`:

````diff
diff --git a/ui/src/pipelines/Pipeline.test.ts b/ui/src/pipelines/Pipeline.test.ts
index 624fed46ef5d09a02af44dbf5a888924fcfa8619..8083a872345bd6d62ab271f3857a5db6b55672ad 100644
--- a/ui/src/pipelines/Pipeline.test.ts
+++ b/ui/src/pipelines/Pipeline.test.ts
@@ -53,7 +53,7 @@ function client(): Client & { sent: unknown[][] } {
 const session = (role: Role): Session => ({ name: 'Oleksii', role, auth: 'basic', expires: null, signed_in: null, csrf: 't' });
 
 async function render(
-  over: { role?: Role; offline?: boolean; missing?: boolean; stale?: boolean; last?: number | null; read?: Partial<PipelinePage['read']>; api?: Client } = {},
+  over: { role?: Role; offline?: boolean; missing?: boolean; stale?: boolean; last?: number | null; read?: Partial<PipelinePage['read']>; cluster?: Model['read']['cluster']; api?: Client } = {},
 ) {
   const api = over.api ?? client();
   const page = new PipelinePage(api, 'orders-pg', liveFigures(), () => NOW);
@@ -64,6 +64,7 @@ async function render(
   Object.assign(page.read, over.read ?? {});
   const model = new Model(api);
   model.facts = facts();
+  if (over.cluster !== undefined) model.read.cluster = over.cluster;
   const target = document.createElement('div');
   document.body.replaceChildren(target);
   mounted = mount(Pipeline, {
@@ -230,6 +231,15 @@ describe("a pipeline's page", () => {
     );
   });
 
+  it("says when the cluster's leases couldn't be read, and counts its lease down no more", async () => {
+    const t = await render({ cluster: { at: NOW - 30_000, error: 'HTTP 503' } });
+    expect(t.querySelector('.notice.failing')?.textContent).toBe(
+      "Couldn't be read; showing what was last read: the cluster (HTTP 503), last read 30 s ago",
+    );
+    expect(t.querySelector('.where')?.textContent).toContain('lease epoch 2');
+    expect(t.querySelector('.where')?.textContent).not.toMatch(/expire/);
+  });
+
   it('says a part that failed and was never read is never read', async () => {
     const t = await render({ read: { events: { at: null, error: 'HTTP 503' } } });
     expect(t.querySelector('.notice.failing')?.textContent).toContain('its activity (HTTP 503), never read');
````

`ui/src/pipelines/detail.test.ts`:

````diff
diff --git a/ui/src/pipelines/detail.test.ts b/ui/src/pipelines/detail.test.ts
index a4893ed253acc8a5382886aaf2df270c3286ca68..4316dd2cc96be9c04d5f425940d4261f3c57f90e 100644
--- a/ui/src/pipelines/detail.test.ts
+++ b/ui/src/pipelines/detail.test.ts
@@ -21,6 +21,14 @@ describe("a pipeline's page", () => {
     expect(where(NOW + 7_000)).toContain('expires in 1 s');
   });
 
+  it("counts nothing down once the cluster's read fails: its lease's end is only what the last read saw", () => {
+    const failed = { at: NOW - 30_000, error: 'HTTP 503' };
+    expect(header(detail(), undefined, liveFigures(), lease(), NOW + 23_000, failed)!.where).toBe(
+      'on n2 · away from owner n1 · lease epoch 2',
+    );
+    expect(header(detail(), undefined, liveFigures(), lease(), NOW, { at: NOW, error: null })!.where).toContain('expires in 8 s');
+  });
+
   it('says a released lease is released, with no epoch to count down', () => {
     const d = detail({ holder: { ...detail().holder!, released: true } });
     expect(header(d, undefined, null, undefined, NOW)!.where).toBe('lease released');
````

- [ ] **Step 2: Run them to see them fail**

Run: `(cd ui && npm test)`
Expected: 10 failed, 144 passed: `live.test.ts`'s `names the node it comes through …`; five in
`model.test.ts` (the reads lack `/v1/cluster/removals`, `node` and `PART_NAMES` aren't there);
`attention.test.ts`'s removal; `Overview.test.ts`'s notice; `detail.test.ts`'s and
`Pipeline.test.ts`'s failed cluster read.

- [ ] **Step 3: Implement**

The model's part, follow and prune; `Live.via`; the rule; the Overview, the list and a
pipeline's page word their failing notice through `lib/notices.ts` with `PART_NAMES`; the
header counts nothing down while `clusterRead.error` is set, and a pipeline's page names the
cluster among its failed parts.

`ui/src/lib/live.svelte.ts`:

````diff
diff --git a/ui/src/lib/live.svelte.ts b/ui/src/lib/live.svelte.ts
index fc080dac62a0ee7504da3517c4336780f88d129b..f97606c9e7f58fba1563e664b9c98cebc8fe050b 100644
--- a/ui/src/lib/live.svelte.ts
+++ b/ui/src/lib/live.svelte.ts
@@ -50,6 +50,8 @@ export class Live {
   state = $state<State>('connecting');
   /** When the last message came (ms since the epoch); `null` before the first. */
   last = $state<number | null>(null);
+  /** The node the stream comes through, as its last hello named it; `null` before the first. */
+  via = $state<string | null>(null);
 
   #deps: Deps;
   #url: string;
@@ -157,7 +159,10 @@ export class Live {
     this.last = Date.now();
     this.#reopens = 0;
     this.state = 'live';
-    if (kind === 'hello' && this.#hellos++ > 0) this.#resyncAll();
+    if (kind === 'hello') {
+      this.via = (data as { node?: string }).node ?? null;
+      if (this.#hellos++ > 0) this.#resyncAll();
+    }
     if (kind === 'reset') {
       const reason = (data as { reason?: string }).reason;
       if (reason === 'lagged') this.#resyncAll();
````

`ui/src/model/model.svelte.ts`:

````diff
diff --git a/ui/src/model/model.svelte.ts b/ui/src/model/model.svelte.ts
index 4b869897bbc83948721d447600b6ff6675945dcc..0084eca051160fbe3765d237bdbf96036cf6c913 100644
--- a/ui/src/model/model.svelte.ts
+++ b/ui/src/model/model.svelte.ts
@@ -5,7 +5,7 @@
 import type { Client } from '../lib/api';
 import type { Live } from '../lib/live.svelte';
 import { LEASE_LOSS_WITHIN_S } from '../overview/attention';
-import { empty, type Facts } from './facts';
+import { at, empty, type Facts } from './facts';
 import type {
   Actual,
   ClusterStatus,
@@ -13,7 +13,9 @@ import type {
   EventItem,
   Figures,
   Job,
+  NodeFigures,
   PipelineItem,
+  Removal,
   Series,
   Tables,
 } from './types';
@@ -41,7 +43,8 @@ export type Part =
   | 'events'
   | 'actions'
   | 'leases'
-  | 'lag';
+  | 'lag'
+  | 'removals';
 export const PARTS: Part[] = [
   'pipelines',
   'cluster',
@@ -52,8 +55,23 @@ export const PARTS: Part[] = [
   'actions',
   'leases',
   'lag',
+  'removals',
 ];
 
+/** Each part as people read it, for the notices that say which couldn't be read. */
+export const PART_NAMES: Record<Part, string> = {
+  pipelines: 'the pipelines',
+  cluster: 'the cluster',
+  config: "the nodes' config",
+  jobs: 'the snapshot jobs',
+  tables: 'the tables',
+  events: 'the events',
+  actions: "the pipelines' last actions",
+  leases: "the hour's lease hand-offs",
+  lag: 'the lag lines',
+  removals: 'the pending removals',
+};
+
 /** When a part was last read (ms), and why its last read failed. */
 export interface Read {
   at: number | null;
@@ -74,7 +92,8 @@ function only<T>(r: Record<string, T>, keep: Set<string>): Record<string, T> {
 /** The parts an event's kind changes. */
 function partsOf(kind: string): Part[] {
   if (kind.startsWith('job.')) return ['jobs'];
-  if (kind.startsWith('config.')) return ['config'];
+  if (kind.startsWith('config.')) return ['config', 'removals'];
+  if (kind === 'pipeline.removed') return ['pipelines', 'removals'];
   if (kind.startsWith('lease.')) return ['cluster', 'pipelines', 'leases'];
   if (kind.startsWith('member.')) return ['cluster', 'pipelines'];
   if (kind.startsWith('table.') || kind.startsWith('schema.')) return ['tables'];
@@ -113,6 +132,7 @@ export class Model {
       live.on('event', (d) => this.event(d as EventItem)),
       live.on('members', () => this.#reread(['cluster'])),
       live.on('peer', (d) => this.peer(d as PeerMsg)),
+      live.on('node', (d) => this.node(d as NodeFigures)),
     ];
     return () => {
       for (const stop of stops) stop();
@@ -120,6 +140,13 @@ export class Model {
     };
   }
 
+  /** A node's own figures, every 5 s: the newest it sent wins. */
+  node(m: NodeFigures): void {
+    const had = this.facts.nodes[m.node];
+    if (had !== undefined && (at(had.at) ?? 0) > (at(m.at) ?? 0)) return;
+    this.facts.nodes[m.node] = m;
+  }
+
   /** A pipeline's state changed on the node that runs it; `standby` when it stopped running it. */
   pipeline(m: PipelineMsg): void {
     const p = this.facts.pipelines.find((x) => x.id === m.pipeline);
@@ -206,7 +233,9 @@ export class Model {
           }
           f.unheld = unheld;
           // A member removed gets no more `peer`: it is out of reach no longer.
-          f.unreachable = only(f.unreachable, new Set(f.cluster.members.map((m) => m.node)));
+          const members = new Set(f.cluster.members.map((m) => m.node));
+          f.unreachable = only(f.unreachable, members);
+          f.nodes = only(f.nodes, members);
           break;
         }
         case 'config':
@@ -240,6 +269,9 @@ export class Model {
           f.leaseLosses = events;
           break;
         }
+        case 'removals':
+          f.removals = (await this.#api.get<{ removals: Removal[] }>('/v1/cluster/removals')).removals;
+          break;
         case 'lag': {
           const { series } = await this.#api.get<{ series: Series[] }>('/v1/history?series=lag_seconds');
           f.lag = Object.fromEntries(
````

`ui/src/overview/Overview.svelte`:

````diff
diff --git a/ui/src/overview/Overview.svelte b/ui/src/overview/Overview.svelte
index 57e8d9d30300e3ca29b939f0f5b8786f5c5d41ee..bce77f497d92028df9b6c8a0cfe4227e84f8aea9 100644
--- a/ui/src/overview/Overview.svelte
+++ b/ui/src/overview/Overview.svelte
@@ -7,6 +7,7 @@
     LAG_TARGET_S,
     LEASE_LOSS_WITHIN_S,
     PAUSED_FOR_S,
+    REMOVAL_PENDING_S,
     RISING_OVER_S,
     UNHELD_FOR_S,
     WAL_SHARE,
@@ -14,9 +15,9 @@
     attention,
     nodeHref,
   } from './attention';
-  import { staleNotice } from '../lib/notices';
+  import { failingNotice, failures, staleNotice } from '../lib/notices';
   import { rows, tiles } from './derive';
-  import { EVENTS_SHOWN, PARTS, type Model } from '../model/model.svelte';
+  import { EVENTS_SHOWN, PART_NAMES, PARTS, type Model } from '../model/model.svelte';
   import Sparkline from './Sparkline.svelte';
 
   interface Props {
@@ -32,9 +33,7 @@
   const shown = $derived(tiles(model.facts, now));
   const table = $derived(rows(model.facts, now));
   const events = $derived(model.facts.events.slice(0, EVENTS_SHOWN));
-  const failing = $derived(
-    PARTS.filter((p) => model.read[p].error !== null).map((p) => `${p} (${model.read[p].error})`),
-  );
+  const failing = $derived(failingNotice(failures(PARTS.map((p) => [PART_NAMES[p], model.read[p]]), now)));
   const href = (kind: string, id: string) => `/ui/${kind}/${encodeURIComponent(id)}`;
 </script>
 
@@ -42,8 +41,8 @@
 {#if stale}
   <p class="notice stale-note" role="status">{staleNotice(last, now)}</p>
 {/if}
-{#if failing.length > 0}
-  <p class="notice failing" role="alert">Couldn't be read, shown as last read: {failing.join('; ')}</p>
+{#if failing !== null}
+  <p class="notice failing" role="alert">{failing}</p>
 {/if}
 
 <div class="tiles" class:stale>
@@ -76,7 +75,7 @@
   {WAL_SHARE * 100}% of max_slot_wal_keep_size, or {bytes(WAL_UNLIMITED_BYTES)} without one ·
   paused or stopped by a person for over {PAUSED_FOR_S / 3600} h · snapshot jobs failed in the last
   {FAILED_WITHIN_S / 3600} h · leases lost in the last {LEASE_LOSS_WITHIN_S / 3600} h, or with no
-  holder for over {UNHELD_FOR_S} s
+  holder for over {UNHELD_FOR_S} s · a removal pending for over {REMOVAL_PENDING_S / 60} min
 </p>
 
 <div class="two">
````

`ui/src/overview/attention.ts`:

````diff
diff --git a/ui/src/overview/attention.ts b/ui/src/overview/attention.ts
index 33ddb1c107536a5bc4144da3876e05194864f956..233cce6631cad7ce9e09b7b8ad2edc3c2687cb9d 100644
--- a/ui/src/overview/attention.ts
+++ b/ui/src/overview/attention.ts
@@ -22,6 +22,8 @@ export const FAILED_WITHIN_S = 86_400;
 export const LEASE_LOSS_WITHIN_S = 3600;
 /** A lease with no holder for longer than this: a rebalance hands leases over within it. */
 export const UNHELD_FOR_S = 30;
+/** A removal is torn down at once by its owner, else within the ring's next sweeps (every 30 s). */
+export const REMOVAL_PENDING_S = 120;
 
 export type Severity = 'red' | 'orange' | 'yellow';
 
@@ -206,8 +208,8 @@ export function attention(f: Facts, now: number): Attention[] {
     });
   }
 
-  // Yellow: nodes on different config generations; restart-only changes waiting; a pipeline
-  // paused or stopped by a person for a while.
+  // Yellow: nodes on different config generations; restart-only changes waiting; a removal
+  // waiting for its teardown; a pipeline paused or stopped by a person for a while.
   for (const n of f.config?.nodes ?? []) {
     if (f.config !== null && n.generation !== null && n.generation !== f.config.generation) {
       add({
@@ -231,6 +233,19 @@ export function attention(f: Facts, now: number): Attention[] {
       });
     }
   }
+  for (const r of f.removals) {
+    const since = at(r.removed_at);
+    if (since !== null && now - since > REMOVAL_PENDING_S * 1000) {
+      add({
+        severity: 'yellow',
+        what: `Removal of ${r.id} pending`,
+        why: `its teardown hasn't finished since generation ${r.generation} removed it`,
+        since,
+        href: '/ui/cluster',
+        link: 'Cluster',
+      });
+    }
+  }
   for (const p of f.pipelines) {
     const a = f.actions[p.id];
     const when = at(a?.at);
````

`ui/src/pipelines/List.svelte`:

````diff
diff --git a/ui/src/pipelines/List.svelte b/ui/src/pipelines/List.svelte
index ab2899881c73ba48f6011003723e03f31210dc6e..29e47edacd91f0bea11f67c297aa38525d2cef8d 100644
--- a/ui/src/pipelines/List.svelte
+++ b/ui/src/pipelines/List.svelte
@@ -20,7 +20,7 @@
   import type { Router } from '../lib/router.svelte';
   import { formatValue } from '../lib/series';
   import type { Session } from '../lib/session';
-  import type { Model } from '../model/model.svelte';
+  import { PART_NAMES, type Model, type Part } from '../model/model.svelte';
   import { LAG_TARGET_S } from '../overview/attention';
   import Sparkline from '../overview/Sparkline.svelte';
   import type { LagHistory } from './lag.svelte';
@@ -157,19 +157,10 @@
     return outcomeText(o);
   }
 
-  /** The parts the list shows, as people call them. */
+  /** The parts the list shows. */
+  const LIST_PARTS: Part[] = ['pipelines', 'cluster', 'jobs', 'tables'];
   const failing = $derived(
-    failingNotice(
-      failures(
-        [
-          ['the pipelines', model.read.pipelines],
-          ['the cluster', model.read.cluster],
-          ['the snapshot jobs', model.read.jobs],
-          ['the tables', model.read.tables],
-        ],
-        now,
-      ),
-    ),
+    failingNotice(failures(LIST_PARTS.map((p) => [PART_NAMES[p], model.read[p]]), now)),
   );
   const headings: { column: Column | null; label: string }[] = [
     { column: 'pipeline', label: 'Pipeline' },
````

`ui/src/pipelines/Pipeline.svelte`:

````diff
diff --git a/ui/src/pipelines/Pipeline.svelte b/ui/src/pipelines/Pipeline.svelte
index 92fb44d8b00c69b1ea412a739c8ae0067318f677..9367db430fe5e444a24c7bf01b5a03c319fda1ea 100644
--- a/ui/src/pipelines/Pipeline.svelte
+++ b/ui/src/pipelines/Pipeline.svelte
@@ -9,10 +9,10 @@
   import Chart from '../lib/Chart.svelte';
   import Dialog from '../lib/Dialog.svelte';
   import { ago } from '../lib/format';
-  import { failingNotice, failures, staleNotice } from '../lib/notices';
+  import { failingNotice, failures, staleNotice, type PartRead } from '../lib/notices';
   import type { Session } from '../lib/session';
   import { at } from '../model/facts';
-  import { CLUSTER_REREAD_MS, type Model } from '../model/model.svelte';
+  import { CLUSTER_REREAD_MS, PART_NAMES as MODEL_PART_NAMES, type Model } from '../model/model.svelte';
   import { LAG_TARGET_S } from '../overview/attention';
   import { activity, header, marks, snapshotPanel, sourceLines, tableLines } from './detail';
   import { SPAN_S } from './lag.svelte';
@@ -33,7 +33,7 @@
   const id = $derived(page.id);
   const item = $derived(model.facts.pipelines.find((p) => p.id === id));
   const lease = $derived(model.facts.cluster?.pipelines.find((l) => l.id === id));
-  const head = $derived(header(page.detail, item, page.figures, lease, now));
+  const head = $derived(header(page.detail, item, page.figures, lease, now, model.read.cluster));
   const tables = $derived(tableLines(id, page.figures, page.detail, page.events, now));
   const source = $derived(sourceLines(page.figures));
   const snap = $derived(snapshotPanel(page.jobs, now));
@@ -43,7 +43,18 @@
   const desired = $derived(page.detail?.desired_state ?? item?.desired_state ?? null);
   const can = $derived(mayAct(session.role) && desired !== null);
   const offered = $derived(desired === null ? [] : VERBS.filter((v) => ACTIONS[v].takes(desired)));
-  const failing = $derived(failingNotice(failures(PAGE_PARTS.map((p) => [PART_NAMES[p], page.read[p]]), now)));
+  // The lease in the header comes from the cluster the console reads every 5 s.
+  const failing = $derived(
+    failingNotice(
+      failures(
+        [
+          ...PAGE_PARTS.map((p): [string, PartRead] => [PART_NAMES[p], page.read[p]]),
+          [MODEL_PART_NAMES.cluster, model.read.cluster],
+        ],
+        now,
+      ),
+    ),
+  );
   const readAt = $derived(at(page.figures?.source?.read_at));
 
   onMount(() => {
````

`ui/src/pipelines/detail.ts`:

````diff
diff --git a/ui/src/pipelines/detail.ts b/ui/src/pipelines/detail.ts
index 0abb0de5a8eb4c3ac366f3989bcca71c7464da72..0c969c15fc4e991020ad9c234a4e2112daad0f1f 100644
--- a/ui/src/pipelines/detail.ts
+++ b/ui/src/pipelines/detail.ts
@@ -2,6 +2,7 @@
 // header, the tables, the source, the snapshot, the chart's events and the activity.
 
 import { ago, bytes, clock, duration } from '../lib/format';
+import type { PartRead } from '../lib/notices';
 import { compact, formatValue, type Mark } from '../lib/series';
 import { at, route } from '../model/facts';
 import type { ClusterStatus, EventItem, Figures, JobDetail, PipelineDetail, PipelineItem } from '../model/types';
@@ -30,12 +31,17 @@ export interface Header {
   where: string;
 }
 
+/**
+ * `clusterRead`: the cluster's last read; once it fails, the lease's end is only what the last
+ * good read saw, so nothing is counted down.
+ */
 export function header(
   d: PipelineDetail | null,
   item: PipelineItem | undefined,
   fig: Figures | null,
   lease: ClusterStatus['pipelines'][number] | undefined,
   now: number,
+  clusterRead: PartRead = { at: null, error: null },
 ): Header | null {
   const p = d ?? item;
   if (p === undefined) return null;
@@ -56,7 +62,7 @@ export function header(
   }
   const epoch = fig?.owner_epoch ?? d?.holder?.owner_epoch ?? lease?.owner_epoch;
   if (node !== null && epoch !== undefined && epoch !== null) {
-    const ends = at(lease?.expires_at);
+    const ends = clusterRead.error === null ? at(lease?.expires_at) : null;
     const left = ends === null ? '' : ends > now ? `, expires in ${duration((ends - now) / 1000)}` : `, expired ${duration((now - ends) / 1000)} ago`;
     parts.push(`lease epoch ${epoch}${left}`);
   }
````

- [ ] **Step 4: Run the tests and the gates**

Run `(cd ui && npm test)`: 154 passed. Then

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
git add -A ui/src/lib/live.svelte.ts ui/src/lib/live.test.ts ui/src/model/facts.ts ui/src/model/fixtures.ts ui/src/model/model.svelte.ts ui/src/model/model.test.ts ui/src/model/types.ts ui/src/overview/Overview.svelte ui/src/overview/Overview.test.ts ui/src/overview/attention.test.ts ui/src/overview/attention.ts ui/src/pipelines/List.svelte ui/src/pipelines/Pipeline.svelte ui/src/pipelines/Pipeline.test.ts ui/src/pipelines/detail.test.ts ui/src/pipelines/detail.ts
git commit -m "feat(ui): the model reads the pending removals and follows each node's figures; a removal pending over 2 min needs attention; one wording for every page's notices, the cluster's read among a pipeline page's"
```

---

### Task 3: The Cluster page (`ui/src/cluster`, `lib/Tiles.svelte`)

**Files:**
- Modify: `ui/src/App.svelte`
- Modify: `ui/src/App.test.ts`
- Create: `ui/src/cluster/Cluster.svelte`
- Create: `ui/src/cluster/Cluster.test.ts`
- Create: `ui/src/cluster/derive.test.ts`
- Create: `ui/src/cluster/derive.ts`
- Create: `ui/src/cluster/renewals.svelte.ts`
- Create: `ui/src/cluster/renewals.test.ts`
- Create: `ui/src/lib/Tiles.svelte`
- Modify: `ui/src/lib/series.test.ts`
- Modify: `ui/src/lib/series.ts`
- Modify: `ui/src/model/fixtures.ts`
- Modify: `ui/src/model/types.ts`
- Modify: `ui/src/overview/Overview.svelte`

**Interfaces:**
- Consumes: Task 2's model (`facts.cluster.settings`, `facts.removals`, `facts.leaseLosses`,
  `facts.unreachable`, `PART_NAMES`), M15's `Tile` and the Overview's tiles markup, M16a's
  `notices.ts` and `formatValue`.
- Produces (D-M16b.3): `cluster/derive.ts`'s `stateOf(m: { suspect: boolean; phase: string }):
  { label; tone }`, `AWAY_RED_AFTER_S = 3600`, `clusterTiles(f, now): Tile[]`,
  `memberRows(f, renewals: Record<string, number>, now): MemberRow[]`,
  `awayRows(f, now): AwayRow[]`, `leaseRows(f, now): LeaseRow[]`, `removalRows(f, now):
  RemovalRow[]`; `cluster/renewals.svelte.ts`'s `RENEWALS_SERIES`, `class Renewals(api, now?)`
  with `max`, `read` and `load(nodes: string[])`; `lib/Tiles.svelte` (`tiles`, `stale`);
  `Cluster.svelte` (`model`, `renewals`, `now`, `stale`, `last`); `App.svelte` shows it at
  `/ui/cluster`. `formatValue('seconds', v)` writes `38 ms` under a second.

- [ ] **Step 1: Write the failing tests**

The tiles, members, away rows, leases and removals as mockup 03 shows them; the renewals'
reads; the page's panels, notices and stale dimming; the app reads each member's renewals once as
the page opens (`App.test.ts` answers the parts it reads); `38 ms`.

`ui/src/App.test.ts`:

````diff
diff --git a/ui/src/App.test.ts b/ui/src/App.test.ts
index 4565db24fff96c226f11cb6d764fb3e1de069b95..4798509084137e24428e9edc0e458c520b2161d0 100644
--- a/ui/src/App.test.ts
+++ b/ui/src/App.test.ts
@@ -53,7 +53,7 @@ describe('the app', () => {
   });
 
   /** The console as the server answers it, for pipelines `a` and `b`; `asked` lists the reads. */
-  function serve(session: () => Session) {
+  function serve(session: () => Session, cluster: unknown = { members: [], pipelines: [] }) {
     const asked: string[] = [];
     const json = (body: unknown) => new Response(JSON.stringify(body), { status: 200 });
     const fetch = vi.fn(async (input: RequestInfo | URL) => {
@@ -62,7 +62,11 @@ describe('the app', () => {
       if (path === '/ui/session') return json(session());
       if (path === '/ui/session/refresh') return json(session());
       if (path === '/v1/pipelines') return json({ pipelines: facts().pipelines });
-      if (path === '/v1/cluster/status') return json({ members: [], pipelines: [] });
+      if (path === '/v1/cluster/status') return json(cluster);
+      if (path === '/v1/cluster/removals') return json({ removals: [] });
+      if (path === '/v1/snapshots') return json({ jobs: [] });
+      if (path === '/v1/config/nodes') return json(facts().config);
+      if (path === '/v1/tables') return json({ tables: [], unreachable: [] });
       const one = /^\/v1\/pipelines\/([^/]+)(\/[a-z]+)?$/.exec(path);
       if (one !== null) {
         const id = one[1];
@@ -154,4 +158,12 @@ describe('the app', () => {
     expect(reads()).toBe(1);
     expect(t.querySelector('h2')).toBe(h2);
   });
+
+  it("reads each member's lease renewals once as the Cluster page opens, not again at each answer", async () => {
+    const asked = serve(operator, { members: facts().cluster!.members, pipelines: [] });
+    await open('/ui/cluster');
+    await settle();
+    const renewals = asked.filter((p) => p.includes('series=lease_renew_seconds')).sort();
+    expect(renewals).toEqual(['n1', 'n2', 'n3'].map((n) => `/v1/history?node=${n}&series=lease_renew_seconds`));
+  });
 });
````

`ui/src/cluster/Cluster.test.ts`:

````diff
diff --git a/ui/src/cluster/Cluster.test.ts b/ui/src/cluster/Cluster.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..c50a87641a3b56f2cefce04d3309f04b262d2095
--- /dev/null
+++ b/ui/src/cluster/Cluster.test.ts
@@ -0,0 +1,86 @@
+import { flushSync, mount, tick, unmount } from 'svelte';
+import { afterEach, describe, expect, it } from 'vitest';
+import type { Client } from '../lib/api';
+import { facts, iso, NOW } from '../model/fixtures';
+import { Model } from '../model/model.svelte';
+import Cluster from './Cluster.svelte';
+import { Renewals } from './renewals.svelte';
+
+let mounted: ReturnType<typeof mount> | undefined;
+afterEach(() => {
+  if (mounted !== undefined) unmount(mounted);
+  mounted = undefined;
+});
+
+const api: Client = {
+  get: async <T,>() => ({ series: [] }) as T,
+  send: async () => {
+    throw new Error('nothing is changed');
+  },
+};
+
+async function render(over: { stale?: boolean; prepare?: (m: Model, r: Renewals) => void } = {}): Promise<HTMLElement> {
+  const model = new Model(api);
+  model.facts = facts();
+  const renewals = new Renewals(api, () => NOW);
+  renewals.max = { n1: 0.038, n2: 0.61 };
+  over.prepare?.(model, renewals);
+  const target = document.createElement('div');
+  document.body.replaceChildren(target);
+  mounted = mount(Cluster, { target, props: { model, renewals, now: NOW, stale: over.stale ?? false, last: NOW - 34_000 } });
+  await tick();
+  flushSync();
+  return target;
+}
+
+const text = (e: Element | null | undefined) => e?.textContent?.replace(/\s+/g, ' ').trim();
+const rows = (t: HTMLElement, section: string) =>
+  [...t.querySelectorAll(`section.${section} tbody tr`)].map((r) => [...r.querySelectorAll('td')].map((c) => text(c)));
+
+describe('the Cluster page', () => {
+  it("shows mockup 03's page: tiles, members, away from owner, leases", async () => {
+    const t = await render();
+    expect(text(t.querySelector('h2'))).toBe('Cluster');
+    expect([...t.querySelectorAll('.tile .k')].map((k) => text(k))).toEqual([
+      'Members',
+      'Leases',
+      'Config generation',
+      'Away from owner',
+      'Cluster',
+    ]);
+    expect(rows(t, 'members')[0]).toEqual(['n1', 'alive', '10.0.1.11:8080', '1.1.0', '42', '2 held · owns 3, 1 away · 1 snapshot', '38 ms', '3d 0h']);
+    expect(t.querySelector<HTMLAnchorElement>('section.members tbody a')?.getAttribute('href')).toBe('/ui/cluster/nodes/n1');
+    expect(rows(t, 'away')).toEqual([['orders-pg', 'n2', 'n1', '—', 'away for over an hour: no hand-off since', 'once n1 has been in the ring 15 s, 1 hand-off at a time']]);
+    expect(rows(t, 'leases').map((r) => r[0])).toEqual(['orders-pg', 'billing-mysql', 'customers', 'inventory']);
+    expect(t.querySelector('section.removals')).toBeNull();
+  });
+
+  it('shows no away panel while every pipeline is on its owner', async () => {
+    const t = await render({ prepare: (m) => (m.facts.cluster!.pipelines[0].ring_owner = 'n2') });
+    expect(t.querySelector('section.away')).toBeNull();
+  });
+
+  it('lists the pending removals, with what the rule waits for', async () => {
+    const t = await render({ prepare: (m) => (m.facts.removals = [{ id: 'payments', generation: 41, removed_at: iso(NOW - 4 * 60_000) }]) });
+    expect(rows(t, 'removals')).toEqual([['payments', '41', '4 min ago']]);
+    expect(text(t.querySelector('section.removals p'))).toContain('2 min');
+  });
+
+  it("says which part couldn't be read and how old it is, a member's renewals among them", async () => {
+    const t = await render({
+      prepare: (m, r) => {
+        m.read.cluster = { at: NOW - 60_000, error: 'HTTP 503' };
+        r.read = { n2: { at: null, error: 'the request timed out' } };
+      },
+    });
+    expect(text(t.querySelector('.notice.failing'))).toBe(
+      "Couldn't be read; showing what was last read: the cluster (HTTP 503), last read 1 min ago; n2's lease renewals (the request timed out), never read",
+    );
+  });
+
+  it('dims every panel and says how old they are while the stream is stale', async () => {
+    const t = await render({ stale: true });
+    expect(text(t.querySelector('.notice'))).toMatch(/^Not current: as of \d\d:\d\d:\d\d · 34 s old$/);
+    expect([...t.querySelectorAll('.tiles, section')].every((s) => s.classList.contains('stale'))).toBe(true);
+  });
+});
````

`ui/src/cluster/derive.test.ts`:

````diff
diff --git a/ui/src/cluster/derive.test.ts b/ui/src/cluster/derive.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..a143a50a7ed3120599368e7c55ca88f3fa076ec7
--- /dev/null
+++ b/ui/src/cluster/derive.test.ts
@@ -0,0 +1,113 @@
+import { describe, expect, it } from 'vitest';
+import { clock } from '../lib/format';
+import { event, facts, iso, NOW } from '../model/fixtures';
+import { awayRows, clusterTiles, leaseRows, memberRows, removalRows } from './derive';
+
+const MIN = 60_000;
+const takeover = () =>
+  event({ kind: 'lease.takeover', at: iso(NOW - 7 * MIN), pipeline: 'orders-pg', text: 'orders-pg taken over from n1 by n2' });
+
+describe('the cluster', () => {
+  it("heads with mockup 03's tiles: members, leases, the config generation, away from owner, the cluster's settings", () => {
+    const f = facts();
+    f.leaseLosses = [takeover()];
+    expect(clusterTiles(f, NOW)).toEqual([
+      { key: 'Members', value: '3 of 3', detail: 'all alive', tone: 'green' },
+      { key: 'Leases', value: '4', detail: 'all held', tone: 'normal' },
+      { key: 'Config generation', value: '42', detail: 'n3 still on 41', tone: 'orange' },
+      {
+        key: 'Away from owner',
+        value: '1',
+        detail: `last taken over at ${clock(NOW - 7 * MIN)} · 1 lost or taken over in 1 h`,
+        tone: 'orange',
+      },
+      { key: 'Cluster', value: '128 tokens', detail: 'lease 15 s · lock: storage', tone: 'normal' },
+    ]);
+  });
+
+  it('counts members not alive and leases with no holder in the tiles', () => {
+    const f = facts();
+    f.cluster!.members[1].suspect = true;
+    f.cluster!.members[2].phase = 'leaving';
+    f.cluster!.pipelines[3] = { ...f.cluster!.pipelines[3], holder: null, expires_at: null };
+    const [members, leases] = clusterTiles(f, NOW);
+    expect(members).toEqual({ key: 'Members', value: '1 of 3', detail: '1 suspect · 1 leaving', tone: 'orange' });
+    expect(leases).toEqual({ key: 'Leases', value: '4', detail: '1 with no holder', tone: 'red' });
+  });
+
+  it('lists each member: its state, address, version, generation, pipelines held and owned, lease renewal, how long it is up', () => {
+    const rows = memberRows(facts(), { n1: 0.038, n2: 0.61 }, NOW);
+    expect(rows.map((r) => [r.node, r.href, r.state.label, r.address, r.build, r.generation.value, r.pipelines, r.renewal, r.up])).toEqual([
+      ['n1', '/ui/cluster/nodes/n1', 'alive', '10.0.1.11:8080', '1.1.0', '42', '2 held · owns 3, 1 away · 1 snapshot', '38 ms', '3d 0h'],
+      ['n2', '/ui/cluster/nodes/n2', 'alive', '10.0.1.12:8080', '1.1.0', '42', '1 held · owns 0 · 1 not its own', '610 ms', '3d 0h'],
+      ['n3', '/ui/cluster/nodes/n3', 'alive', '10.0.1.13:8080', '1.1.0', '41', '1 held · owns 1', '—', '2h 0m'],
+    ]);
+    expect(rows.map((r) => r.generation.tone)).toEqual(['normal', 'normal', 'orange']);
+  });
+
+  it("says a member is suspect, leaving, or out of the console's reach, in words and colour", () => {
+    const f = facts();
+    f.cluster!.members[1].suspect = true;
+    f.cluster!.members[2].phase = 'leaving';
+    f.unreachable = { n1: NOW - 40_000 };
+    expect(memberRows(f, {}, NOW).map((r) => [r.node, r.state.label, r.state.tone])).toEqual([
+      ['n1', 'alive · unreachable 40 s', 'orange'],
+      ['n2', 'suspect', 'orange'],
+      ['n3', 'leaving', 'grey'],
+    ]);
+  });
+
+  it('lists the pipelines held away from their owner: since when and why, and when they go back', () => {
+    const f = facts();
+    f.leaseLosses = [takeover()];
+    expect(awayRows(f, NOW)).toEqual([
+      {
+        id: 'orders-pg',
+        href: '/ui/pipelines/orders-pg',
+        holder: 'n2',
+        owner: 'n1',
+        since: NOW - 7 * MIN,
+        why: 'orders-pg taken over from n1 by n2',
+        back: 'once n1 has been in the ring 15 s, 1 hand-off at a time',
+        tone: 'orange',
+      },
+    ]);
+  });
+
+  it('turns an away pipeline red when no hand-off in the last hour explains it', () => {
+    const [row] = awayRows(facts(), NOW);
+    expect([row.since, row.why, row.tone]).toEqual([null, 'away for over an hour: no hand-off since', 'red']);
+  });
+
+  it('lists every lease: no holder first, then away from owner, then the soonest to end', () => {
+    const f = facts();
+    f.cluster!.pipelines[3] = { ...f.cluster!.pipelines[3], holder: null, expires_at: null };
+    f.cluster!.pipelines[1].expires_at = iso(NOW + 3_000);
+    f.cluster!.pipelines[2].expires_at = iso(NOW - 2_000);
+    expect(leaseRows(f, NOW).map((r) => [r.id, r.holder, r.owner, r.epoch, r.expires, r.state.label, r.state.tone])).toEqual([
+      ['customers', null, 'n1', null, '—', 'no holder', 'red'],
+      ['orders-pg', 'n2', 'n1', 2, 'in 8 s', 'away from owner', 'orange'],
+      ['inventory', 'n3', 'n3', 1, 'expired 2 s ago', 'held', 'normal'],
+      ['billing-mysql', 'n1', 'n1', 3, 'in 3 s', 'held', 'normal'],
+    ]);
+  });
+
+  it('says a holder that stopped renewing is not renewing', () => {
+    const f = facts();
+    f.cluster!.pipelines[1].holder_stale = true;
+    const row = leaseRows(f, NOW).find((r) => r.id === 'billing-mysql')!;
+    expect(row.state).toEqual({ label: 'not renewing', tone: 'red' });
+  });
+
+  it('lists the removals waiting for their teardown, oldest first, past the threshold in orange', () => {
+    const f = facts();
+    f.removals = [
+      { id: 'payments', generation: 41, removed_at: iso(NOW - 4 * MIN) },
+      { id: 'audit', generation: 42, removed_at: iso(NOW - 30_000) },
+    ];
+    expect(removalRows(f, NOW)).toEqual([
+      { id: 'payments', generation: 41, since: '4 min ago', tone: 'orange' },
+      { id: 'audit', generation: 42, since: '30 s ago', tone: 'normal' },
+    ]);
+  });
+});
````

`ui/src/cluster/renewals.test.ts`:

````diff
diff --git a/ui/src/cluster/renewals.test.ts b/ui/src/cluster/renewals.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..3238cf590a0ce042076107afc6a0914d729d44c2
--- /dev/null
+++ b/ui/src/cluster/renewals.test.ts
@@ -0,0 +1,50 @@
+import { describe, expect, it } from 'vitest';
+import { HttpError, type Client } from '../lib/api';
+import { NOW } from '../model/fixtures';
+import { RENEWALS_SERIES, Renewals } from './renewals.svelte';
+
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
+      throw new Error('nothing is changed');
+    },
+  };
+}
+
+const t = NOW / 1000;
+
+describe("the members' lease renewals", () => {
+  it("reads each member's over the last hour, each on its own, and keeps its slowest", async () => {
+    const a = api({
+      [`/v1/history?node=n1&series=${RENEWALS_SERIES}`]: {
+        series: [{ name: RENEWALS_SERIES, node: 'n1', points: [[t - 120, 0.021], [t - 60, 0.038], [t, 0.012]] }],
+      },
+      [`/v1/history?node=n3&series=${RENEWALS_SERIES}`]: { series: [{ name: RENEWALS_SERIES, node: 'n3', points: [] }] },
+    });
+    const r = new Renewals(a, () => NOW);
+    await r.load(['n1', 'n2', 'n3']);
+    expect(r.max).toEqual({ n1: 0.038 });
+    expect(r.read.n1).toEqual({ at: NOW, error: null });
+    expect(r.read.n2).toEqual({ at: null, error: `/v1/history?node=n2&series=${RENEWALS_SERIES} is unavailable` });
+    expect(r.read.n3).toEqual({ at: NOW, error: null });
+  });
+
+  it('keeps what it had for a member whose read fails, and drops a node no longer a member', async () => {
+    const one = `/v1/history?node=n1&series=${RENEWALS_SERIES}`;
+    const answers: Record<string, unknown> = { [one]: { series: [{ name: RENEWALS_SERIES, node: 'n1', points: [[t, 0.04]] }] } };
+    const r = new Renewals(api(answers), () => NOW);
+    await r.load(['n1', 'n4']);
+    delete answers[one];
+    await r.load(['n1']);
+    expect(r.max).toEqual({ n1: 0.04 });
+    expect(r.read.n1.error).toBe(`${one} is unavailable`);
+    expect(Object.keys(r.read)).toEqual(['n1']);
+  });
+});
````

`ui/src/lib/series.test.ts`:

````diff
diff --git a/ui/src/lib/series.test.ts b/ui/src/lib/series.test.ts
index b0549b0ae5cb1cbf950e6aa002d102c7a774336e..d01090359436426b4b4250596c61269558f9e1c0 100644
--- a/ui/src/lib/series.test.ts
+++ b/ui/src/lib/series.test.ts
@@ -59,6 +59,9 @@ describe("a chart's values", () => {
   it('are written in their units, short', () => {
     expect(formatValue('seconds', 94)).toBe('94 s');
     expect(formatValue('seconds', 252)).toBe('4m 12s');
+    // A lease renewal, under a second: in milliseconds.
+    expect(formatValue('seconds', 0.038)).toBe('38 ms');
+    expect(formatValue('seconds', 0)).toBe('0 s');
     expect(formatValue('bytes', 210 * 1024 ** 2)).toBe('210 MB');
     expect(formatValue('rows/s', 41_200)).toBe('41 k/s');
     expect(formatValue('changes/min', 2_600_000)).toBe('2.6 M/min');
````

`ui/src/model/fixtures.ts`:

````diff
diff --git a/ui/src/model/fixtures.ts b/ui/src/model/fixtures.ts
index d7cc4f54705f3629ca68f22f13b36b6640c6f79f..e678ee84eb2944467408c78c362ed6d99b5dcfd0 100644
--- a/ui/src/model/fixtures.ts
+++ b/ui/src/model/fixtures.ts
@@ -130,6 +130,8 @@ export function facts(): Facts {
         config_generation: 42,
         build: '1.1.0',
         api_url: `https://10.0.1.1${i + 1}:8080`,
+        // n3 restarted two hours ago; the others have been up three days.
+        started_at: iso(node === 'n3' ? NOW - 2 * 60 * MIN : NOW - 3 * 24 * 60 * MIN),
       })),
       pipelines: [
         // Held by n2 away from its ring owner, since a failover.
````

`ui/src/model/types.ts`:

````diff
diff --git a/ui/src/model/types.ts b/ui/src/model/types.ts
index 18daac32b7c0c861044cd5eebc51bb92de86beda..0edf106bf813a48b6265f901381828b30f611ef7 100644
--- a/ui/src/model/types.ts
+++ b/ui/src/model/types.ts
@@ -41,6 +41,8 @@ export interface Member {
   config_generation: number | null;
   build?: string;
   api_url?: string | null;
+  /** When its process started (RFC 3339). */
+  started_at?: string | null;
 }
 
 /** The cluster's settings a console states (`GET /v1/cluster/status`'s `settings`). */
@@ -205,6 +207,7 @@ export interface EventItem {
 export interface Series {
   name: string;
   pipeline?: string;
+  node?: string;
   points: [number, number][];
 }
 
````

- [ ] **Step 2: Run them to see them fail**

Run: `(cd ui && npm test)`
Expected: 2 failed, 153 passed, and three files that can't load (`cluster/derive.test.ts`,
`renewals.test.ts`, `Cluster.test.ts`: their modules aren't there): `series.test.ts`'s units
(`0.038` is `0 s`), and `App.test.ts`'s renewals (none asked: `/ui/cluster` is the "later"
page).

- [ ] **Step 3: Implement**

The derivations, `Renewals`, the page and the shared tiles. In `App.svelte` the renewals
follow the members' names, and `load` runs under `untrack`: it reads and writes `max` and `read`,
which would make the effect run again at each answer, for ever (Global Constraints).

`ui/src/App.svelte`:

````diff
diff --git a/ui/src/App.svelte b/ui/src/App.svelte
index 12b74c52ad89009f13776a561b42f6bcbe5e186b..fb1c3664c39255c8fd2b989078aae2573ec0a0ad 100644
--- a/ui/src/App.svelte
+++ b/ui/src/App.svelte
@@ -1,6 +1,8 @@
 <script lang="ts">
   // The console's app: who is signed in, the live stream, and the page the address names.
   import { onMount, untrack } from 'svelte';
+  import Cluster from './cluster/Cluster.svelte';
+  import { Renewals } from './cluster/renewals.svelte';
   import Frame from './Frame.svelte';
   import Later from './Later.svelte';
   import { client } from './lib/api';
@@ -32,6 +34,17 @@
   const model = new Model(api);
   /** Every pipeline's lag over 6 hours, read when the list is first shown. */
   const lag = new LagHistory(api);
+  /** Each member's lease renewals over the last hour, read each time the Cluster page is shown. */
+  const renewals = new Renewals(api);
+  const onCluster = $derived(router.section === 'cluster' && router.rest.length === 0);
+  /** The members' names: read again when they change, not at each read of the cluster. */
+  const memberNames = $derived((model.facts.cluster?.members ?? []).map((m) => m.node).join('\n'));
+  $effect(() => {
+    if (!onCluster || !signedIn || memberNames === '') return;
+    const names = memberNames.split('\n');
+    // The read follows the members, not the renewals it writes.
+    untrack(() => void renewals.load(names));
+  });
   /** The pipeline whose page is shown; its page reads and follows it while shown. */
   const pageId = $derived(router.section === 'pipelines' && router.rest.length === 1 ? router.rest[0] : null);
   /** Signed in or not: a token refresh swaps the session's object, not who is signed in. */
@@ -100,6 +113,8 @@
   >
     {#if router.section === 'overview'}
       <Overview {model} {now} stale={live.stale(now)} last={live.last} />
+    {:else if onCluster}
+      <Cluster {model} {renewals} {now} stale={live.stale(now)} last={live.last} />
     {:else if page !== null && page.id === pageId}
       <!-- Each pipeline's page starts clean: nothing chosen or open carries over from the last. -->
       {#key page}
````

`ui/src/cluster/Cluster.svelte`:

````diff
diff --git a/ui/src/cluster/Cluster.svelte b/ui/src/cluster/Cluster.svelte
new file mode 100644
index 0000000000000000000000000000000000000000..9e69dcac5bc08cafbdee4f90faba056c7e767263
--- /dev/null
+++ b/ui/src/cluster/Cluster.svelte
@@ -0,0 +1,204 @@
+<script lang="ts">
+  // The Cluster page (Admin_Console §4, mockup 03): its members, the pipelines held away from
+  // their ring owner, every lease, and the removals waiting for their teardown (DA-API-21).
+  import { clock } from '../lib/format';
+  import { failingNotice, failures, staleNotice, type PartRead } from '../lib/notices';
+  import Tiles from '../lib/Tiles.svelte';
+  import { PART_NAMES, type Model, type Part } from '../model/model.svelte';
+  import { REMOVAL_PENDING_S } from '../overview/attention';
+  import { awayRows, clusterTiles, leaseRows, memberRows, removalRows } from './derive';
+  import type { Renewals } from './renewals.svelte';
+
+  interface Props {
+    model: Model;
+    renewals: Renewals;
+    now: number;
+    /** The live updates stopped: every number is as of `last`. */
+    stale: boolean;
+    last: number | null;
+  }
+  let { model, renewals, now, stale, last }: Props = $props();
+
+  /** The parts the page shows. */
+  const PAGE_PARTS: Part[] = ['cluster', 'config', 'jobs', 'leases', 'removals'];
+
+  const tiles = $derived(clusterTiles(model.facts, now));
+  const members = $derived(memberRows(model.facts, renewals.max, now));
+  const away = $derived(awayRows(model.facts, now));
+  const leases = $derived(leaseRows(model.facts, now));
+  const removals = $derived(removalRows(model.facts, now));
+  const failing = $derived(
+    failingNotice(
+      failures(
+        [
+          ...PAGE_PARTS.map((p): [string, PartRead] => [PART_NAMES[p], model.read[p]]),
+          ...Object.entries(renewals.read).map(([n, r]): [string, PartRead] => [`${n}'s lease renewals`, r]),
+        ],
+        now,
+      ),
+    ),
+  );
+</script>
+
+<h2>Cluster</h2>
+{#if stale}
+  <p class="notice" role="status">{staleNotice(last, now)}</p>
+{/if}
+{#if failing !== null}
+  <p class="notice failing" role="alert">{failing}</p>
+{/if}
+
+<Tiles {tiles} {stale} />
+
+<section class="panel members" class:stale>
+  <h3>Members</h3>
+  <table>
+    <thead>
+      <tr>
+        <th>Node</th><th>State</th><th>Address</th><th>Version</th><th>Config</th><th>Pipelines</th>
+        <th>Lease renewal, slowest 1 h</th><th>Up</th>
+      </tr>
+    </thead>
+    <tbody>
+      {#each members as m (m.node)}
+        <tr class={m.state.tone}>
+          <td><a href={m.href}>{m.node}</a></td>
+          <td class={m.state.tone}>{m.state.label}</td>
+          <td>{m.address}</td>
+          <td>{m.build}</td>
+          <td class={m.generation.tone}>{m.generation.value}</td>
+          <td>{m.pipelines}</td>
+          <td>{m.renewal}</td>
+          <td>{m.up}</td>
+        </tr>
+      {/each}
+    </tbody>
+  </table>
+  <p class="muted">
+    States: alive · suspect (missed probes; stays in the ring) · leaving (handing off) · joining. A
+    node the console's stream can't reach says for how long.
+  </p>
+</section>
+
+{#if away.length > 0}
+  <section class="panel away" class:stale>
+    <h3>Away from their owner · {away.length}</h3>
+    <table>
+      <thead>
+        <tr><th>Pipeline</th><th>Held by</th><th>Owner</th><th>Since</th><th>Why</th><th>Goes back</th></tr>
+      </thead>
+      <tbody>
+        {#each away as a (a.id)}
+          <tr class={a.tone}>
+            <td><a href={a.href}>{a.id}</a></td>
+            <td>{a.holder}</td>
+            <td>{a.owner}</td>
+            <td>{a.since === null ? '—' : clock(a.since)}</td>
+            <td>{a.why}</td>
+            <td>{a.back}</td>
+          </tr>
+        {/each}
+      </tbody>
+    </table>
+    <p class="muted">
+      Shown only while something is away; red once one has been away over an hour with no hand-off
+      to say why.
+    </p>
+  </section>
+{/if}
+
+<section class="panel leases" class:stale>
+  <h3>Leases · {leases.length}</h3>
+  <table>
+    <thead>
+      <tr><th>Pipeline</th><th>Holder</th><th>Ring owner</th><th>Epoch</th><th>Expires</th><th>State</th></tr>
+    </thead>
+    <tbody>
+      {#each leases as l (l.id)}
+        <tr>
+          <td><a href={l.href}>{l.id}</a></td>
+          <td>{l.holder ?? '—'}</td>
+          <td>{l.owner ?? '—'}</td>
+          <td>{l.epoch ?? '—'}</td>
+          <td>{l.expires}</td>
+          <td class={l.state.tone}>{l.state.label}</td>
+        </tr>
+      {/each}
+    </tbody>
+  </table>
+</section>
+
+{#if removals.length > 0}
+  <section class="panel removals" class:stale>
+    <h3>Pending removals · {removals.length}</h3>
+    <table>
+      <thead>
+        <tr><th>Pipeline</th><th>Removed by generation</th><th>Since</th></tr>
+      </thead>
+      <tbody>
+        {#each removals as r (r.id)}
+          <tr>
+            <td>{r.id}</td>
+            <td>{r.generation}</td>
+            <td class={r.tone}>{r.since}</td>
+          </tr>
+        {/each}
+      </tbody>
+    </table>
+    <p class="muted">
+      Its owner tears a removed pipeline down; one no node owns, the node the ring places it on
+      within a sweep (every 30 s). Needs attention lists one still pending after
+      {REMOVAL_PENDING_S / 60} min.
+    </p>
+  </section>
+{/if}
+
+<style>
+  .notice {
+    padding: 6px 10px;
+    border-radius: 6px;
+    background: var(--grey-bg);
+    margin: 0 0 10px;
+  }
+  .notice.failing {
+    background: var(--orange-bg);
+  }
+  .stale {
+    opacity: 0.55;
+  }
+  .panel {
+    background: var(--bg-panel);
+    border: 1px solid var(--border);
+    border-radius: 8px;
+    padding: 10px 12px;
+    margin: 12px 0;
+  }
+  .panel h3 {
+    margin: 0 0 8px;
+    font-size: 14px;
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
+  td.orange,
+  tr.orange td:first-child {
+    color: var(--orange);
+  }
+  td.red,
+  tr.red td:first-child {
+    color: var(--red);
+  }
+  td.green {
+    color: var(--green);
+  }
+  .muted {
+    font-size: 12px;
+  }
+</style>
````

`ui/src/cluster/derive.ts`:

````diff
diff --git a/ui/src/cluster/derive.ts b/ui/src/cluster/derive.ts
new file mode 100644
index 0000000000000000000000000000000000000000..3a5b41df206e058a4677c4d8452d57b2248a289b
--- /dev/null
+++ b/ui/src/cluster/derive.ts
@@ -0,0 +1,236 @@
+// The Cluster page (Admin_Console §4, mockup 03), worked out from what the console knows: the
+// tiles, the members, the pipelines held away from their owner, the leases, the pending removals.
+
+import { ago, clock, duration } from '../lib/format';
+import { formatValue } from '../lib/series';
+import { at, type Facts } from '../model/facts';
+import { LEASE_LOSS_WITHIN_S, nodeHref, REMOVAL_PENDING_S } from '../overview/attention';
+import type { Tile, Tone } from '../overview/derive';
+
+/** A pipeline away from its owner for longer than this, with no hand-off to say why, is red. */
+export const AWAY_RED_AFTER_S = 3600;
+
+const pipelineHref = (id: string) => `/ui/pipelines/${encodeURIComponent(id)}`;
+
+/** A member's state in words: SWIM's suspicion first, then its phase. */
+function stateOf(m: { suspect: boolean; phase: string }): { label: string; tone: Tone } {
+  if (m.suspect) return { label: 'suspect', tone: 'orange' };
+  if (m.phase === 'active') return { label: 'alive', tone: 'green' };
+  return { label: m.phase === 'draining' ? 'leaving' : m.phase, tone: 'grey' };
+}
+
+/** The five tiles: members, leases, the config generation, away from owner, the settings. */
+export function clusterTiles(f: Facts, _now: number): Tile[] {
+  const members = f.cluster?.members ?? [];
+  const alive = members.filter((m) => stateOf(m).label === 'alive').length;
+  const suspect = members.filter((m) => m.suspect).length;
+  const leaving = members.filter((m) => !m.suspect && stateOf(m).label === 'leaving').length;
+  const others = [suspect > 0 ? `${suspect} suspect` : null, leaving > 0 ? `${leaving} leaving` : null].filter((x) => x !== null);
+  const leases = f.cluster?.pipelines ?? [];
+  const unheld = leases.filter((l) => l.holder === null).length;
+  const behind = (f.config?.nodes ?? []).filter((n) => n.generation !== null && n.generation !== f.config?.generation);
+  const away = leases.filter((l) => l.holder !== null && l.ring_owner !== null && l.holder !== l.ring_owner).length;
+  const last = f.leaseLosses[0];
+  const lastAt = at(last?.at);
+  const s = f.cluster?.settings;
+  return [
+    {
+      key: 'Members',
+      value: `${alive} of ${members.length}`,
+      detail: others.length === 0 ? 'all alive' : others.join(' · '),
+      tone: others.length === 0 ? 'green' : 'orange',
+    },
+    {
+      key: 'Leases',
+      value: String(leases.length),
+      detail: unheld === 0 ? 'all held' : `${unheld} with no holder`,
+      tone: unheld === 0 ? 'normal' : 'red',
+    },
+    {
+      key: 'Config generation',
+      value: f.config === null ? '—' : String(f.config.generation),
+      detail: behind.length === 0 ? 'every node on it' : behind.map((n) => `${n.node} still on ${n.generation}`).join(' · '),
+      tone: behind.length === 0 ? 'normal' : 'orange',
+    },
+    {
+      key: 'Away from owner',
+      value: String(away),
+      detail:
+        lastAt === null
+          ? `no lease lost or taken over in ${LEASE_LOSS_WITHIN_S / 3600} h`
+          : `last taken over at ${clock(lastAt)} · ${f.leaseLosses.length} lost or taken over in ${LEASE_LOSS_WITHIN_S / 3600} h`,
+      tone: away === 0 ? 'normal' : 'orange',
+    },
+    {
+      key: 'Cluster',
+      value: s === undefined ? '—' : `${s.tokens} tokens`,
+      detail: s === undefined ? '' : `lease ${s.lease_seconds} s · lock: ${s.lock_backend}`,
+      tone: 'normal',
+    },
+  ];
+}
+
+export interface MemberRow {
+  node: string;
+  href: string;
+  state: { label: string; tone: Tone };
+  /** Where its API answers. */
+  address: string;
+  build: string;
+  generation: { value: string; tone: Tone };
+  /** `3 held · owns 5, 2 away · 1 snapshot`. */
+  pipelines: string;
+  /** Its slowest lease renewal in the last hour. */
+  renewal: string;
+  up: string;
+}
+
+/** Each member, in name order. `renewals`: each one's slowest lease renewal (s), where read. */
+export function memberRows(f: Facts, renewals: Record<string, number>, now: number): MemberRow[] {
+  const leases = f.cluster?.pipelines ?? [];
+  const running = new Set(f.jobs.filter((j) => j.status === 'running').map((j) => j.pipeline));
+  return [...(f.cluster?.members ?? [])]
+    .sort((a, b) => a.node.localeCompare(b.node))
+    .map((m) => {
+      const held = leases.filter((l) => l.holder === m.node);
+      const owns = leases.filter((l) => l.ring_owner === m.node);
+      const away = owns.filter((l) => l.holder !== null && l.holder !== m.node).length;
+      const notOwn = held.filter((l) => l.ring_owner !== m.node).length;
+      const snapshots = held.filter((l) => running.has(l.id)).length;
+      const parts = [`${held.length} held`, `owns ${owns.length}${away > 0 ? `, ${away} away` : ''}`];
+      if (notOwn > 0) parts.push(`${notOwn} not its own`);
+      if (snapshots > 0) parts.push(`${snapshots} snapshot${snapshots === 1 ? '' : 's'}`);
+      let state = stateOf(m);
+      const unreachable = f.unreachable[m.node];
+      if (unreachable !== undefined) {
+        state = { label: `${state.label} · unreachable ${duration((now - unreachable) / 1000)}`, tone: 'orange' };
+      }
+      const known = f.config?.nodes.find((n) => n.node === m.node)?.generation ?? m.config_generation;
+      const started = at(m.started_at);
+      const renewal = renewals[m.node];
+      return {
+        node: m.node,
+        href: nodeHref(m.node),
+        state,
+        address: m.api_url == null ? (m.addr ?? '—') : m.api_url.replace(/^https?:\/\//, ''),
+        build: m.build ?? '—',
+        generation: {
+          value: known === null ? '—' : String(known),
+          tone: f.config !== null && known !== null && known !== f.config.generation ? 'orange' : 'normal',
+        },
+        pipelines: parts.join(' · '),
+        renewal: renewal === undefined ? '—' : formatValue('seconds', renewal),
+        up: started === null ? '—' : duration((now - started) / 1000),
+      };
+    });
+}
+
+export interface AwayRow {
+  id: string;
+  href: string;
+  holder: string;
+  owner: string;
+  /** When the hand-off that put it there happened (ms), when the last hour's events say. */
+  since: number | null;
+  why: string;
+  /** When it goes back to its owner. */
+  back: string;
+  tone: Tone;
+}
+
+/** The pipelines held away from their ring owner, the longest away first. */
+export function awayRows(f: Facts, now: number): AwayRow[] {
+  const s = f.cluster?.settings;
+  return (f.cluster?.pipelines ?? [])
+    .filter((l) => l.holder !== null && l.ring_owner !== null && l.holder !== l.ring_owner)
+    .map((l) => {
+      const e = f.leaseLosses.find((x) => x.pipeline === l.id);
+      const since = at(e?.at);
+      const handoffs = s?.max_concurrent_handoffs ?? 1;
+      return {
+        id: l.id,
+        href: pipelineHref(l.id),
+        holder: l.holder!,
+        owner: l.ring_owner!,
+        since,
+        why: e?.text ?? 'away for over an hour: no hand-off since',
+        back:
+          s === undefined
+            ? `once ${l.ring_owner} is stable in the ring`
+            : `once ${l.ring_owner} has been in the ring ${s.rebalance_stabilization_seconds} s, ${handoffs} hand-off${handoffs === 1 ? '' : 's'} at a time`,
+        tone: since === null || now - since > AWAY_RED_AFTER_S * 1000 ? 'red' : 'orange',
+      } satisfies AwayRow;
+    })
+    .sort((a, b) => (a.since ?? -Infinity) - (b.since ?? -Infinity) || a.id.localeCompare(b.id));
+}
+
+export interface LeaseRow {
+  id: string;
+  href: string;
+  holder: string | null;
+  owner: string | null;
+  epoch: number | null;
+  /** `in 8 s`, `expired 2 s ago`, or `—` with no holder. */
+  expires: string;
+  state: { label: string; tone: Tone };
+}
+
+const LEASE_RANK = { 'no holder': 0, 'not renewing': 0, 'away from owner': 1, held: 2 } as const;
+
+/** Every lease: no holder (or one not renewing) first, then away from owner, then the soonest to end. */
+export function leaseRows(f: Facts, now: number): LeaseRow[] {
+  return (f.cluster?.pipelines ?? [])
+    .map((l) => {
+      const ends = at(l.expires_at);
+      const state: LeaseRow['state'] =
+        l.holder === null
+          ? { label: 'no holder', tone: 'red' }
+          : l.holder_stale
+            ? { label: 'not renewing', tone: 'red' }
+            : l.ring_owner !== null && l.holder !== l.ring_owner
+              ? { label: 'away from owner', tone: 'orange' }
+              : { label: 'held', tone: 'normal' };
+      return {
+        row: {
+          id: l.id,
+          href: pipelineHref(l.id),
+          holder: l.holder,
+          owner: l.ring_owner,
+          epoch: l.holder === null ? null : l.owner_epoch,
+          expires:
+            l.holder === null || ends === null
+              ? '—'
+              : ends > now
+                ? `in ${duration((ends - now) / 1000)}`
+                : `expired ${duration((now - ends) / 1000)} ago`,
+          state,
+        },
+        rank: LEASE_RANK[state.label as keyof typeof LEASE_RANK],
+        ends: ends ?? Infinity,
+      };
+    })
+    .sort((a, b) => a.rank - b.rank || a.ends - b.ends || a.row.id.localeCompare(b.row.id))
+    .map((x) => x.row);
+}
+
+export interface RemovalRow {
+  id: string;
+  generation: number;
+  since: string;
+  tone: Tone;
+}
+
+/** The removals waiting for their teardown, the oldest first: past the rule's threshold, orange. */
+export function removalRows(f: Facts, now: number): RemovalRow[] {
+  return [...f.removals]
+    .sort((a, b) => (at(a.removed_at) ?? 0) - (at(b.removed_at) ?? 0))
+    .map((r) => {
+      const since = at(r.removed_at) ?? now;
+      return {
+        id: r.id,
+        generation: r.generation,
+        since: ago(since, now),
+        tone: now - since > REMOVAL_PENDING_S * 1000 ? 'orange' : 'normal',
+      };
+    });
+}
````

`ui/src/cluster/renewals.svelte.ts`:

````diff
diff --git a/ui/src/cluster/renewals.svelte.ts b/ui/src/cluster/renewals.svelte.ts
new file mode 100644
index 0000000000000000000000000000000000000000..9ef0fa986a1172bf456b16274c9deb74aa2454d9
--- /dev/null
+++ b/ui/src/cluster/renewals.svelte.ts
@@ -0,0 +1,44 @@
+// Each member's lease renewals over the last hour (Admin_Console §4, mockup 03's "Lease renewal"),
+// read from the chart history one node at a time: a node's series are asked of it by name.
+
+import type { Client } from '../lib/api';
+import type { PartRead } from '../lib/notices';
+import type { Series } from '../model/types';
+
+/** The node series the history keeps of its lease renewals (seconds, the step's slowest). */
+export const RENEWALS_SERIES = 'lease_renew_seconds';
+
+export class Renewals {
+  /** Each member's slowest renewal in the last hour (s); a member with no sample has none. */
+  max = $state<Record<string, number>>({});
+  read = $state<Record<string, PartRead>>({});
+
+  #api: Client;
+  #now: () => number;
+
+  constructor(api: Client, now: () => number = Date.now) {
+    this.#api = api;
+    this.#now = now;
+  }
+
+  /** Reads `nodes`' renewals, each on its own; forgets a node not among them. */
+  async load(nodes: string[]): Promise<void> {
+    const keep = new Set(nodes);
+    this.max = Object.fromEntries(Object.entries(this.max).filter(([n]) => keep.has(n)));
+    this.read = Object.fromEntries(Object.entries(this.read).filter(([n]) => keep.has(n)));
+    await Promise.all(nodes.map((n) => this.#loadOne(n)));
+  }
+
+  async #loadOne(node: string): Promise<void> {
+    const path = `/v1/history?node=${encodeURIComponent(node)}&series=${RENEWALS_SERIES}`;
+    try {
+      const { series } = await this.#api.get<{ series: Series[] }>(path);
+      const points = series.find((s) => s.name === RENEWALS_SERIES)?.points ?? [];
+      if (points.length === 0) delete this.max[node];
+      else this.max[node] = Math.max(...points.map(([, v]) => v));
+      this.read[node] = { at: this.#now(), error: null };
+    } catch (e) {
+      this.read[node] = { at: this.read[node]?.at ?? null, error: e instanceof Error ? e.message : String(e) };
+    }
+  }
+}
````

`ui/src/lib/Tiles.svelte`:

````diff
diff --git a/ui/src/lib/Tiles.svelte b/ui/src/lib/Tiles.svelte
new file mode 100644
index 0000000000000000000000000000000000000000..10d32a4a029d80336d95a29fb7979f899672a068
--- /dev/null
+++ b/ui/src/lib/Tiles.svelte
@@ -0,0 +1,54 @@
+<script lang="ts">
+  // A page's headline tiles (mockups 01 and 03): each a key, a value and a line under it, its
+  // value coloured by its tone and its tone said in words too.
+  import type { Tile } from '../overview/derive';
+
+  let { tiles, stale = false }: { tiles: Tile[]; stale?: boolean } = $props();
+</script>
+
+<div class="tiles" class:stale>
+  {#each tiles as t (t.key)}
+    <div class="tile {t.tone}">
+      <div class="k">{t.key}</div>
+      <div class="v">{t.value}</div>
+      <div class="s">{t.detail}</div>
+    </div>
+  {/each}
+</div>
+
+<style>
+  .tiles {
+    display: grid;
+    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
+    gap: 10px;
+  }
+  .stale {
+    opacity: 0.55;
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
+</style>
````

`ui/src/lib/series.ts`:

````diff
diff --git a/ui/src/lib/series.ts b/ui/src/lib/series.ts
index 714e7813f5468f2886bde29f42624832336c05bd..43fc8fce78076b7d910217c5cf088077bd7070b2 100644
--- a/ui/src/lib/series.ts
+++ b/ui/src/lib/series.ts
@@ -71,6 +71,7 @@ export function compact(n: number): string {
 export function formatValue(unit: Unit, v: number): string {
   switch (unit) {
     case 'seconds':
+      if (v > 0 && v < 1) return `${Math.round(v * 1000)} ms`;
       return v < 120 ? `${Math.round(v)} s` : duration(v);
     case 'bytes':
       return bytes(v);
````

`ui/src/overview/Overview.svelte`:

````diff
diff --git a/ui/src/overview/Overview.svelte b/ui/src/overview/Overview.svelte
index bce77f497d92028df9b6c8a0cfe4227e84f8aea9..483428d8dac89ec174589a585b272de4fc453e05 100644
--- a/ui/src/overview/Overview.svelte
+++ b/ui/src/overview/Overview.svelte
@@ -19,6 +19,7 @@
   import { rows, tiles } from './derive';
   import { EVENTS_SHOWN, PART_NAMES, PARTS, type Model } from '../model/model.svelte';
   import Sparkline from './Sparkline.svelte';
+  import Tiles from '../lib/Tiles.svelte';
 
   interface Props {
     model: Model;
@@ -45,15 +46,7 @@
   <p class="notice failing" role="alert">{failing}</p>
 {/if}
 
-<div class="tiles" class:stale>
-  {#each shown as t (t.key)}
-    <div class="tile {t.tone}">
-      <div class="k">{t.key}</div>
-      <div class="v">{t.value}</div>
-      <div class="s">{t.detail}</div>
-    </div>
-  {/each}
-</div>
+<Tiles tiles={shown} {stale} />
 
 <h3>Needs attention · {list.length}</h3>
 {#if list.length === 0}
@@ -140,37 +133,6 @@
   .stale {
     opacity: 0.55;
   }
-  .tiles {
-    display: grid;
-    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
-    gap: 10px;
-  }
-  .tile {
-    border: 1px solid var(--border);
-    border-radius: 8px;
-    padding: 10px;
-    background: var(--bg-panel);
-  }
-  .k {
-    font-size: 11px;
-    text-transform: uppercase;
-    letter-spacing: 0.04em;
-    color: var(--text-2);
-  }
-  .v {
-    font-size: 22px;
-    font-weight: 600;
-  }
-  .s {
-    font-size: 11px;
-    color: var(--text-2);
-  }
-  .tile.orange .v {
-    color: var(--orange);
-  }
-  .tile.red .v {
-    color: var(--red);
-  }
   .att,
   .ev {
     list-style: none;
````

- [ ] **Step 4: Run the tests and the gates**

Run `(cd ui && npm test)`: 171 passed. Then

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
git add -A ui/src/App.svelte ui/src/App.test.ts ui/src/cluster/Cluster.svelte ui/src/cluster/Cluster.test.ts ui/src/cluster/derive.test.ts ui/src/cluster/derive.ts ui/src/cluster/renewals.svelte.ts ui/src/cluster/renewals.test.ts ui/src/lib/Tiles.svelte ui/src/lib/series.test.ts ui/src/lib/series.ts ui/src/model/fixtures.ts ui/src/model/types.ts ui/src/overview/Overview.svelte
git commit -m "feat(ui): the Cluster page — tiles, members and their lease renewals, pipelines away from their owner, leases, pending removals"
```

---

### Task 4: A node's page (`ui/src/nodes`)

**Files:**
- Modify: `ui/src/App.svelte`
- Modify: `ui/src/App.test.ts`
- Modify: `ui/src/cluster/derive.ts`
- Modify: `ui/src/model/types.ts`
- Create: `ui/src/nodes/Node.svelte`
- Create: `ui/src/nodes/Node.test.ts`
- Create: `ui/src/nodes/fixtures.ts`
- Create: `ui/src/nodes/page.svelte.ts`
- Create: `ui/src/nodes/page.test.ts`
- Create: `ui/src/nodes/view.test.ts`
- Create: `ui/src/nodes/view.ts`

**Interfaces:**
- Consumes: Task 2's `NodeFigures`, `Process`, `Gossip`; Task 3's `stateOf`; M16a's
  `Chart.svelte`, `Sparkline`, `LagHistory`, `pipelines/detail.ts`'s `activity()`, the Overview's
  `rows()`.
- Produces (D-M16b.4): `types.ts`'s `NodeDetail`, `Series.node?`; `nodes/view.ts`'s
  `nodeHeader(name, f, d, now): { state; line } | null`, `nodePipelines(name, f, d, now)`,
  `gossipLines(g, now)`, `renewalNote(points)`, `memoryNote(p)`, `cpuNote(p)`;
  `nodes/page.svelte.ts`'s `NodePart`, `NODE_PARTS`, `NODE_PART_NAMES`, `SPAN_S = 6 * 3600`,
  `STEP_S = 60`, `NODE_SERIES`, `EVENTS_READ = 50`, `class NodePage(api, name, now?)` with
  `detail`, `series`, `events`, `missing`, `read`, `load(parts?)`, `follow(live)`, `event(e)`;
  `Node.svelte` (`page`, `model`, `lag`, `now`, `stale`, `last`); `nodes/fixtures.ts`'s
  `nodeDetail(over?)`, `nodeSeries()`, `nodeEvents()`; `App.svelte` shows it at
  `/ui/cluster/nodes/{name}`.

- [ ] **Step 1: Write the failing tests**

The header from the node's detail or the cluster's status; its pipelines; its gossip
and chart notes; the page's reads, a 404, the stream's events; the page as mockup 04; the app
reads the node's three parts once.

`ui/src/App.test.ts`:

````diff
diff --git a/ui/src/App.test.ts b/ui/src/App.test.ts
index 4798509084137e24428e9edc0e458c520b2161d0..6d36f08de809af19d53a1c675e4c73f4aab2479f 100644
--- a/ui/src/App.test.ts
+++ b/ui/src/App.test.ts
@@ -3,6 +3,7 @@ import { afterEach, describe, expect, it, vi } from 'vitest';
 import App from './App.svelte';
 import { RETRY_MS, type Session } from './lib/session';
 import { facts } from './model/fixtures';
+import { nodeDetail } from './nodes/fixtures';
 import { detail, liveFigures } from './pipelines/fixtures';
 
 // The chart's uPlot, as in the pipeline page's tests.
@@ -66,6 +67,7 @@ describe('the app', () => {
       if (path === '/v1/cluster/removals') return json({ removals: [] });
       if (path === '/v1/snapshots') return json({ jobs: [] });
       if (path === '/v1/config/nodes') return json(facts().config);
+      if (path === '/v1/nodes/n2') return json(nodeDetail());
       if (path === '/v1/tables') return json({ tables: [], unreachable: [] });
       const one = /^\/v1\/pipelines\/([^/]+)(\/[a-z]+)?$/.exec(path);
       if (one !== null) {
@@ -166,4 +168,13 @@ describe('the app', () => {
     const renewals = asked.filter((p) => p.includes('series=lease_renew_seconds')).sort();
     expect(renewals).toEqual(['n1', 'n2', 'n3'].map((n) => `/v1/history?node=${n}&series=lease_renew_seconds`));
   });
+
+  it("reads a node's detail, charts and activity once as its page opens", async () => {
+    const asked = serve(operator, { members: facts().cluster!.members, pipelines: [] });
+    const t = await open('/ui/cluster/nodes/n2');
+    expect(t.querySelector('h2')?.textContent).toContain('n2');
+    expect(asked.filter((p) => p === '/v1/nodes/n2')).toHaveLength(1);
+    expect(asked.filter((p) => p.startsWith('/v1/history?node=n2&series=lease_renew_seconds,'))).toHaveLength(1);
+    expect(asked.filter((p) => p.startsWith('/v1/events?node=n2'))).toHaveLength(1);
+  });
 });
````

`ui/src/model/types.ts`:

````diff
diff --git a/ui/src/model/types.ts b/ui/src/model/types.ts
index 0edf106bf813a48b6265f901381828b30f611ef7..290d222843f40eeca06c44e81b5af36797361903 100644
--- a/ui/src/model/types.ts
+++ b/ui/src/model/types.ts
@@ -274,3 +274,22 @@ export interface NodeFigures {
   /** The pipelines it runs. */
   pipelines: string[];
 }
+
+/** `GET /v1/nodes/{node}`: a node's detail, from the node itself. */
+export interface NodeDetail {
+  node: string;
+  api_url: string | null;
+  gossip_addr: string | null;
+  build: string | null;
+  incarnation: number | null;
+  started_at: string | null;
+  phase: string | null;
+  config_generation: number;
+  /** Its own file against its generation (keyed fingerprints). */
+  file: { hash: string | null; generation_hash: string | null; matches: boolean | null };
+  restart_pending: string[];
+  process: Process;
+  gossip: Gossip;
+  /** The pipelines whose lease it holds; `null` when the leases couldn't be read. */
+  pipelines: { id: string; owner_epoch: number; ring_owner: string | null }[] | null;
+}
````

`ui/src/nodes/Node.test.ts`:

````diff
diff --git a/ui/src/nodes/Node.test.ts b/ui/src/nodes/Node.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..0afb8dd90fee1ac51cd8b5d357a5323ee2e01c35
--- /dev/null
+++ b/ui/src/nodes/Node.test.ts
@@ -0,0 +1,100 @@
+import { flushSync, mount, tick, unmount } from 'svelte';
+import { afterEach, describe, expect, it, vi } from 'vitest';
+import type { Client } from '../lib/api';
+import { facts, NOW } from '../model/fixtures';
+import { Model } from '../model/model.svelte';
+import { LagHistory } from '../pipelines/lag.svelte';
+import { nodeDetail, nodeEvents, nodeSeries } from './fixtures';
+import Node from './Node.svelte';
+import { NodePage } from './page.svelte';
+
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
+const api: Client = {
+  get: async <T,>() => ({}) as T,
+  send: async () => {
+    throw new Error('nothing is changed');
+  },
+};
+
+async function render(over: { missing?: boolean; stale?: boolean; read?: Partial<NodePage['read']> } = {}): Promise<HTMLElement> {
+  const page = new NodePage(api, 'n2', () => NOW);
+  page.detail = nodeDetail();
+  page.series = nodeSeries();
+  page.events = nodeEvents();
+  page.missing = over.missing ?? false;
+  Object.assign(page.read, over.read ?? {});
+  const model = new Model(api);
+  model.facts = facts();
+  const lag = new LagHistory(api);
+  const target = document.createElement('div');
+  document.body.replaceChildren(target);
+  mounted = mount(Node, { target, props: { page, model, lag, now: NOW, stale: over.stale ?? false, last: NOW - 34_000 } });
+  await vi.dynamicImportSettled();
+  await tick();
+  flushSync();
+  return target;
+}
+
+const text = (e: Element | null | undefined) => e?.textContent?.replace(/\s+/g, ' ').trim();
+
+describe("a node's page", () => {
+  it("shows mockup 04's page: its header, its pipelines, its charts, its activity and its gossip", async () => {
+    const t = await render();
+    expect(text(t.querySelector('h2'))).toBe('n2 alive');
+    expect(text(t.querySelector('.where'))).toBe('10.0.1.12:8080 · gossip 10.0.1.12:7946 · 1.1.0 · config gen 42 · started 3 d ago');
+    const row = [...t.querySelectorAll('section.pipelines tbody tr td')].map((c) => text(c));
+    expect([row[0], row[1], row[3]]).toEqual(['orders-pg', '1 halted', 'epoch 2 · away from owner n1']);
+    expect([...t.querySelectorAll('figure.chart figcaption span:first-child')].map((c) => text(c))).toEqual([
+      'Lease renewal, 6 h',
+      'Memory, 6 h',
+      'CPU, 6 h',
+    ]);
+    expect(text(t.querySelector('section.charts'))).toContain('1.3 GB of 2 GB limit');
+    expect([...t.querySelectorAll('.activity li')].map((li) => text(li))).toHaveLength(3);
+    expect([...t.querySelectorAll('section.gossip dt')].map((d) => text(d))).toEqual(['Sees', 'Last probe', 'Suspicions, 24 h']);
+  });
+
+  it('says when no member has its name', async () => {
+    const t = await render({ missing: true });
+    expect(text(t.querySelector('[role="alert"]'))).toBe("There is no member n2 in this node's view of the cluster.");
+  });
+
+  it("says which part couldn't be read and how old it is", async () => {
+    const t = await render({ read: { history: { at: null, error: 'HTTP 503' } } });
+    expect(text(t.querySelector('.notice.failing'))).toBe("Couldn't be read; showing what was last read: its charts (HTTP 503), never read");
+  });
+
+  it('dims every panel and says how old they are while the stream is stale', async () => {
+    const t = await render({ stale: true });
+    expect(text(t.querySelector('.notice'))).toMatch(/^Not current: as of \d\d:\d\d:\d\d · 34 s old$/);
+    expect([...t.querySelectorAll('section')].every((s) => s.classList.contains('stale'))).toBe(true);
+  });
+});
````

`ui/src/nodes/fixtures.ts`:

````diff
diff --git a/ui/src/nodes/fixtures.ts b/ui/src/nodes/fixtures.ts
new file mode 100644
index 0000000000000000000000000000000000000000..1be9da8e09124e217037c78221748431068cce2c
--- /dev/null
+++ b/ui/src/nodes/fixtures.ts
@@ -0,0 +1,54 @@
+// The tests' node n2 (mockup 04's node-b): up three days, holding orders-pg away from its owner
+// n1 since a failover, with a day's suspicion it raised.
+
+import { event, iso, nodeFigures, NOW } from '../model/fixtures';
+import type { EventItem, NodeDetail } from '../model/types';
+
+const MIN = 60_000;
+const GIB = 1024 ** 3;
+const t = NOW / 1000;
+
+export function nodeDetail(over: Partial<NodeDetail> = {}): NodeDetail {
+  const live = nodeFigures('n2');
+  return {
+    node: 'n2',
+    api_url: 'https://10.0.1.12:8080',
+    gossip_addr: '10.0.1.12:7946',
+    build: '1.1.0',
+    incarnation: NOW - 3 * 24 * 60 * MIN,
+    started_at: iso(NOW - 3 * 24 * 60 * MIN),
+    phase: 'active',
+    config_generation: 42,
+    file: { hash: 'a1b2c3d4e5f60718', generation_hash: 'a1b2c3d4e5f60718', matches: true },
+    restart_pending: [],
+    process: live.process,
+    gossip: { ...live.gossip, suspicions: { raised: [{ node: 'n1', at: iso(NOW - 8 * MIN) }], received: 0, raised_more: 0, received_more: 0 } },
+    pipelines: [{ id: 'orders-pg', owner_epoch: 2, ring_owner: 'n1' }],
+    ...over,
+  };
+}
+
+/** n2's charts over 6 hours: a renewal peak at the failover, memory and CPU. */
+export function nodeSeries(): Record<string, [number, number][]> {
+  return {
+    lease_renew_seconds: [
+      [t - 600, 0.041],
+      [t - 420, 0.61],
+      [t, 0.038],
+    ],
+    memory_bytes: [
+      [t - 300, 1.2 * GIB],
+      [t, 1.3 * GIB],
+    ],
+    cpu_cores: [[t, 0.6]],
+  };
+}
+
+/** What n2 recorded, newest first. */
+export function nodeEvents(): EventItem[] {
+  return [
+    event({ key: 'e3', kind: 'lease.takeover', at: iso(NOW - 7 * MIN), node: 'n2', pipeline: 'orders-pg', text: 'orders-pg taken over from n1 by n2' }),
+    event({ key: 'e2', kind: 'member.restarted', at: iso(NOW - 10 * MIN), node: 'n2', object: 'n3', text: 'n3 rejoined' }),
+    event({ key: 'e1', kind: 'config.adopted', at: iso(NOW - 4 * 60 * MIN), node: 'n2', text: 'adopted config generation 42' }),
+  ];
+}
````

`ui/src/nodes/page.test.ts`:

````diff
diff --git a/ui/src/nodes/page.test.ts b/ui/src/nodes/page.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..537d5897ffca6de6128a2e3dccdb217079b70056
--- /dev/null
+++ b/ui/src/nodes/page.test.ts
@@ -0,0 +1,64 @@
+import { describe, expect, it } from 'vitest';
+import { HttpError, type Client } from '../lib/api';
+import { event, iso, NOW } from '../model/fixtures';
+import { nodeDetail, nodeEvents, nodeSeries } from './fixtures';
+import { NODE_SERIES, NodePage, SPAN_S, STEP_S } from './page.svelte';
+
+const HISTORY = `/v1/history?node=n2&series=${NODE_SERIES.join(',')}&since=${iso(NOW - SPAN_S * 1000)}&step=${STEP_S}`;
+
+function api(answers: Record<string, unknown>): Client & { asked: string[] } {
+  const asked: string[] = [];
+  return {
+    asked,
+    get: async <T,>(path: string) => {
+      asked.push(path);
+      const a = answers[path];
+      if (a instanceof HttpError) throw a;
+      if (a === undefined) throw new HttpError(503, `${path} is unavailable`);
+      return a as T;
+    },
+    send: async () => {
+      throw new Error('nothing is changed');
+    },
+  };
+}
+
+const READS = {
+  '/v1/nodes/n2': nodeDetail(),
+  [HISTORY]: { series: Object.entries(nodeSeries()).map(([name, points]) => ({ name, node: 'n2', points })) },
+  '/v1/events?node=n2&limit=50': { events: nodeEvents() },
+};
+
+describe("a node's page, as read", () => {
+  it('reads its detail, its charts over 6 hours and what it recorded, each on its own', async () => {
+    const a = api(READS);
+    const p = new NodePage(a, 'n2', () => NOW);
+    await p.load();
+    expect(a.asked.sort()).toEqual(['/v1/events?node=n2&limit=50', '/v1/nodes/n2', HISTORY].sort());
+    expect(p.detail?.build).toBe('1.1.0');
+    expect(p.series).toEqual(nodeSeries());
+    expect(p.events.map((e) => e.key)).toEqual(['e3', 'e2', 'e1']);
+    expect(Object.values(p.read).every((r) => r.at === NOW && r.error === null)).toBe(true);
+    expect([SPAN_S, STEP_S]).toEqual([6 * 3600, 60]);
+  });
+
+  it('says when no member has its name, and keeps what a failed part had', async () => {
+    const answers: Record<string, unknown> = { ...READS, '/v1/nodes/n2': new HttpError(404, 'no member "n2"') };
+    const p = new NodePage(api(answers), 'n2', () => NOW);
+    await p.load();
+    expect(p.missing).toBe(true);
+    delete answers[HISTORY];
+    await p.load(['history']);
+    expect(p.series).toEqual(nodeSeries());
+    expect(p.read.history.error).toBe(`${HISTORY} is unavailable`);
+  });
+
+  it("adds what the node records as it comes, once, and nothing another node records", () => {
+    const p = new NodePage(api(READS), 'n2', () => NOW);
+    const e = event({ key: 'e4', kind: 'lease.acquired', at: iso(NOW), node: 'n2', text: 'n2 acquired crm-pg' });
+    p.event(e);
+    p.event(e);
+    p.event(event({ key: 'e5', kind: 'lease.acquired', at: iso(NOW), node: 'n1', text: 'n1 acquired billing' }));
+    expect(p.events.map((x) => x.key)).toEqual(['e4']);
+  });
+});
````

`ui/src/nodes/view.test.ts`:

````diff
diff --git a/ui/src/nodes/view.test.ts b/ui/src/nodes/view.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..58e0e9ed860ff3c63e66fb797f856bea7693b67a
--- /dev/null
+++ b/ui/src/nodes/view.test.ts
@@ -0,0 +1,76 @@
+import { describe, expect, it } from 'vitest';
+import { clock } from '../lib/format';
+import { facts, nodeFigures, NOW } from '../model/fixtures';
+import { nodeDetail, nodeSeries } from './fixtures';
+import { cpuNote, gossipLines, memoryNote, nodeHeader, nodePipelines, renewalNote } from './view';
+
+const MIN = 60_000;
+
+describe("a node's page", () => {
+  it('heads with its state, where its API and gossip answer, its build, generation and start', () => {
+    expect(nodeHeader('n2', facts(), nodeDetail(), NOW)).toEqual({
+      state: { label: 'alive', tone: 'green' },
+      line: '10.0.1.12:8080 · gossip 10.0.1.12:7946 · 1.1.0 · config gen 42 · started 3 d ago',
+    });
+  });
+
+  it("heads with what the cluster's status says of it until its own detail is read", () => {
+    expect(nodeHeader('n3', facts(), null, NOW)).toEqual({
+      state: { label: 'alive', tone: 'green' },
+      line: '10.0.1.13:8080 · gossip 10.0.1.13:7946 · 1.1.0 · config gen 41 · started 2 h ago',
+    });
+  });
+
+  it("says a node the console's stream can't reach is unreachable, and a name no member has is none", () => {
+    const f = facts();
+    f.unreachable = { n2: NOW - 40_000 };
+    expect(nodeHeader('n2', f, nodeDetail(), NOW)!.state).toEqual({ label: 'alive · unreachable 40 s', tone: 'orange' });
+    expect(nodeHeader('n9', facts(), null, NOW)).toBeNull();
+  });
+
+  it('lists the pipelines it holds, the worst first, with their lag and their lease', () => {
+    expect(nodePipelines('n2', facts(), nodeDetail(), NOW)).toEqual([
+      {
+        id: 'orders-pg',
+        href: '/ui/pipelines/orders-pg',
+        state: { label: '1 halted', tone: 'red' },
+        lag: '21 s',
+        lease: 'epoch 2 · away from owner n1',
+      },
+    ]);
+    expect(nodePipelines('n1', facts(), null, NOW).map((r) => [r.id, r.state.label, r.lag, r.lease])).toEqual([
+      ['billing-mysql', 'lagging', '4m 12s', 'epoch 3'],
+      ['customers', 'snapshot 62%', '—', 'epoch 1'],
+    ]);
+  });
+
+  it('takes its own detail for what it holds while the cluster is not read yet', () => {
+    const f = facts();
+    f.cluster = null;
+    expect(nodePipelines('n2', f, nodeDetail(), NOW).map((r) => [r.id, r.lease])).toEqual([['orders-pg', 'epoch 2 · away from owner n1']]);
+  });
+
+  it('says what its gossip sees: its peers, its last probe, the suspicions of the last day', () => {
+    expect(gossipLines(nodeDetail().gossip, NOW)).toEqual([
+      { label: 'Sees', value: 'n1 alive · n3 alive' },
+      { label: 'Last probe', value: 'n1 0 s ago · RTT 1.2 ms' },
+      { label: 'Suspicions, 24 h', value: `raised 1 (n1 ${clock(NOW - 8 * MIN)}) · received 0` },
+    ]);
+    const g = nodeFigures('n2', { gossip: { ...nodeDetail().gossip, peers: [{ node: 'n1', state: 'suspect', phase: 'active' }], last_probe: null, probe_rtt_ms: null } }).gossip;
+    expect(gossipLines(g, NOW).slice(0, 2)).toEqual([
+      { label: 'Sees', value: 'n1 suspect' },
+      { label: 'Last probe', value: 'none yet' },
+    ]);
+  });
+
+  it("heads its charts with the renewals' peak, the memory against its limit and the CPU against its", () => {
+    expect(renewalNote(nodeSeries().lease_renew_seconds)).toBe(`peak 610 ms at ${clock(NOW - 7 * MIN)}`);
+    expect(renewalNote([])).toBe('no samples yet');
+    const p = nodeDetail().process;
+    expect(memoryNote(p)).toBe('1.3 GB of 2 GB limit');
+    expect(memoryNote({ ...p, memory_limit_bytes: null })).toBe('1.3 GB · no limit');
+    expect(cpuNote(p)).toBe('0.6 cores · limit 2');
+    expect(cpuNote({ ...p, cpu_limit_cores: null })).toBe('0.6 cores · no limit');
+    expect(cpuNote({ ...p, cpu_cores: null })).toBe('—');
+  });
+});
````

- [ ] **Step 2: Run them to see them fail**

Run: `(cd ui && npm test)`
Expected: 1 failed, 171 passed, and three files that can't load (`nodes/view.test.ts`,
`page.test.ts`, `Node.test.ts`): `App.test.ts`'s node page (no `h2` naming `n2`).

- [ ] **Step 3: Implement**

The view, the page's reads and the component; `App.svelte` makes a `NodePage` when the
name changes, loads it and follows the stream while it is shown.

`ui/src/App.svelte`:

````diff
diff --git a/ui/src/App.svelte b/ui/src/App.svelte
index fb1c3664c39255c8fd2b989078aae2573ec0a0ad..aca6aba0ac305cae8e0c92d9d488527452c0689d 100644
--- a/ui/src/App.svelte
+++ b/ui/src/App.svelte
@@ -10,6 +10,8 @@
   import { Router } from './lib/router.svelte';
   import { awaitSession, checkSession, keepFresh, RETRY_MS, type Session } from './lib/session';
   import { Model } from './model/model.svelte';
+  import Node from './nodes/Node.svelte';
+  import { NodePage } from './nodes/page.svelte';
   import Overview from './overview/Overview.svelte';
   import { LagHistory } from './pipelines/lag.svelte';
   import List from './pipelines/List.svelte';
@@ -50,6 +52,21 @@
   /** Signed in or not: a token refresh swaps the session's object, not who is signed in. */
   const signedIn = $derived(session !== null);
   let page = $state<PipelinePage | null>(null);
+  /** The node whose page is shown: `/ui/cluster/nodes/{name}`. */
+  const nodeName = $derived(
+    router.section === 'cluster' && router.rest.length === 2 && router.rest[0] === 'nodes' ? router.rest[1] : null,
+  );
+  let nodePage = $state<NodePage | null>(null);
+  $effect(() => {
+    if (nodeName === null || !signedIn) {
+      nodePage = null;
+      return;
+    }
+    const p = new NodePage(api, nodeName);
+    nodePage = p;
+    void p.load();
+    return p.follow(live);
+  });
   $effect(() => {
     if (pageId === null || !signedIn) {
       page = null;
@@ -115,6 +132,10 @@
       <Overview {model} {now} stale={live.stale(now)} last={live.last} />
     {:else if onCluster}
       <Cluster {model} {renewals} {now} stale={live.stale(now)} last={live.last} />
+    {:else if nodePage !== null && nodePage.name === nodeName}
+      {#key nodePage}
+        <Node page={nodePage} {model} {lag} {now} stale={live.stale(now)} last={live.last} />
+      {/key}
     {:else if page !== null && page.id === pageId}
       <!-- Each pipeline's page starts clean: nothing chosen or open carries over from the last. -->
       {#key page}
````

`ui/src/cluster/derive.ts`:

````diff
diff --git a/ui/src/cluster/derive.ts b/ui/src/cluster/derive.ts
index 3a5b41df206e058a4677c4d8452d57b2248a289b..78f76dc7b844d5c63c44ae7f881915b5b5b3219a 100644
--- a/ui/src/cluster/derive.ts
+++ b/ui/src/cluster/derive.ts
@@ -13,7 +13,7 @@ export const AWAY_RED_AFTER_S = 3600;
 const pipelineHref = (id: string) => `/ui/pipelines/${encodeURIComponent(id)}`;
 
 /** A member's state in words: SWIM's suspicion first, then its phase. */
-function stateOf(m: { suspect: boolean; phase: string }): { label: string; tone: Tone } {
+export function stateOf(m: { suspect: boolean; phase: string }): { label: string; tone: Tone } {
   if (m.suspect) return { label: 'suspect', tone: 'orange' };
   if (m.phase === 'active') return { label: 'alive', tone: 'green' };
   return { label: m.phase === 'draining' ? 'leaving' : m.phase, tone: 'grey' };
````

`ui/src/nodes/Node.svelte`:

````diff
diff --git a/ui/src/nodes/Node.svelte b/ui/src/nodes/Node.svelte
new file mode 100644
index 0000000000000000000000000000000000000000..fdda520f24a3a0fa5a88b23000a15451ae9f0051
--- /dev/null
+++ b/ui/src/nodes/Node.svelte
@@ -0,0 +1,250 @@
+<script lang="ts">
+  // A node's page (Admin_Console §4, mockup 04): its header, the pipelines it holds, its lease
+  // renewals, memory and CPU over 6 hours, what it recorded, and what its gossip sees.
+  import { onMount } from 'svelte';
+  import Chart from '../lib/Chart.svelte';
+  import { failingNotice, failures, staleNotice, type PartRead } from '../lib/notices';
+  import type { Model } from '../model/model.svelte';
+  import { LAG_TARGET_S } from '../overview/attention';
+  import Sparkline from '../overview/Sparkline.svelte';
+  import { activity } from '../pipelines/detail';
+  import type { LagHistory } from '../pipelines/lag.svelte';
+  import { NODE_PART_NAMES, NODE_PARTS, SPAN_S, type NodePage } from './page.svelte';
+  import { cpuNote, gossipLines, memoryNote, nodeHeader, nodePipelines, renewalNote } from './view';
+
+  interface Props {
+    page: NodePage;
+    model: Model;
+    /** Every pipeline's lag over 6 hours. */
+    lag: LagHistory;
+    now: number;
+    /** The live updates stopped: every number is as of `last`. */
+    stale: boolean;
+    last: number | null;
+  }
+  let { page, model, lag, now, stale, last }: Props = $props();
+
+  const name = $derived(page.name);
+  const head = $derived(nodeHeader(name, model.facts, page.detail, now));
+  const held = $derived(nodePipelines(name, model.facts, page.detail, now));
+  /** The stream's figures are fresher than the node's detail. */
+  const live = $derived(model.facts.nodes[name] ?? page.detail);
+  const since = $derived(now / 1000 - SPAN_S);
+  const lines = $derived(activity(page.events, now));
+  const failing = $derived(
+    failingNotice(failures(NODE_PARTS.map((p): [string, PartRead] => [NODE_PART_NAMES[p], page.read[p]]), now)),
+  );
+  const series = (s: string) => page.series[s] ?? [];
+
+  onMount(() => void lag.ensure());
+</script>
+
+{#if page.missing}
+  <h2>{name}</h2>
+  <p role="alert">There is no member {name} in this node's view of the cluster.</p>
+  <a href="/ui/cluster">The cluster →</a>
+{:else}
+  <h2>
+    {name}
+    {#if head !== null}<span class="chip {head.state.tone}">{head.state.label}</span>{/if}
+  </h2>
+  {#if head !== null}<p class="where muted">{head.line}</p>{/if}
+  {#if stale}
+    <p class="notice" role="status">{staleNotice(last, now)}</p>
+  {/if}
+  {#if failing !== null}
+    <p class="notice failing" role="alert">{failing}</p>
+  {/if}
+
+  <section class="panel pipelines" class:stale>
+    <h3>Pipelines on this node · {held.length}</h3>
+    <table>
+      <thead>
+        <tr><th>Pipeline</th><th>State</th><th>Lag, 6 h</th><th>Lease</th></tr>
+      </thead>
+      <tbody>
+        {#each held as p (p.id)}
+          <tr>
+            <td><a href={p.href}>{p.id}</a></td>
+            <td><span class="chip {p.state.tone}">{p.state.label}</span></td>
+            <td class="lag">
+              <Sparkline points={lag.lines[p.id] ?? []} target={LAG_TARGET_S} />
+              {p.lag}
+            </td>
+            <td>{p.lease}</td>
+          </tr>
+        {:else}
+          <tr><td colspan="4" class="muted">It holds no pipeline.</td></tr>
+        {/each}
+      </tbody>
+    </table>
+  </section>
+
+  <section class="panel charts" class:stale>
+    <h3>This node, 6 h</h3>
+    <div class="three">
+      <div>
+        <p class="note">{renewalNote(series('lease_renew_seconds'))}</p>
+        <Chart label="Lease renewal, 6 h" unit="seconds" points={series('lease_renew_seconds')} {since} until={now / 1000} height={140} />
+      </div>
+      <div>
+        <p class="note">{live === null ? '—' : memoryNote(live.process)}</p>
+        <Chart
+          label="Memory, 6 h"
+          unit="bytes"
+          points={series('memory_bytes')}
+          {since}
+          until={now / 1000}
+          target={live?.process.memory_limit_bytes ?? null}
+          height={140}
+        />
+      </div>
+      <div>
+        <p class="note">{live === null ? '—' : cpuNote(live.process)}</p>
+        <Chart
+          label="CPU, 6 h"
+          unit="cores"
+          points={series('cpu_cores')}
+          {since}
+          until={now / 1000}
+          target={live?.process.cpu_limit_cores ?? null}
+          height={140}
+        />
+      </div>
+    </div>
+  </section>
+
+  <div class="cols">
+    <section class="panel" class:stale>
+      <h3>Activity on this node</h3>
+      <ul class="activity">
+        {#each lines as e (e.key)}
+          <li class={e.severity}>
+            <span class="when">{e.when}</span>
+            <span>{e.text}{#if e.who !== null}<span class="muted"> · {e.who}</span>{/if}{#if e.note !== null}<span class="muted"> "{e.note}"</span>{/if}</span>
+          </li>
+        {:else}
+          <li class="muted">Nothing recorded by it yet.</li>
+        {/each}
+      </ul>
+      <a href="/ui/events?node={encodeURIComponent(name)}">All its events →</a>
+    </section>
+    <section class="panel gossip" class:stale>
+      <h3>Gossip</h3>
+      {#if live === null}
+        <p class="muted">Not read yet.</p>
+      {:else}
+        <dl>
+          {#each gossipLines(live.gossip, now) as g (g.label)}
+            <dt>{g.label}</dt>
+            <dd>{g.value}</dd>
+          {/each}
+        </dl>
+      {/if}
+    </section>
+  </div>
+{/if}
+
+<style>
+  .where {
+    margin-top: -6px;
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
+  .three {
+    display: grid;
+    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
+    gap: 12px;
+  }
+  .note {
+    margin: 0 0 4px;
+    font-size: 12px;
+    color: var(--text-2);
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
+  .lag {
+    display: flex;
+    gap: 8px;
+    align-items: center;
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
+  .activity {
+    list-style: none;
+    padding: 0;
+    margin: 0 0 8px;
+  }
+  .activity li {
+    display: flex;
+    gap: 10px;
+    padding: 3px 4px;
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
+</style>
````

`ui/src/nodes/page.svelte.ts`:

````diff
diff --git a/ui/src/nodes/page.svelte.ts b/ui/src/nodes/page.svelte.ts
new file mode 100644
index 0000000000000000000000000000000000000000..2690a04d91c08983e01dba69b14610885a3ad671
--- /dev/null
+++ b/ui/src/nodes/page.svelte.ts
@@ -0,0 +1,99 @@
+// A node's page (Admin_Console §4, mockup 04): its detail from the node itself, its charts over 6
+// hours from the chart history, and what it recorded. Each part is read on its own and keeps
+// what it had when a read fails; the stream adds what the node records as it comes.
+
+import { HttpError, type Client } from '../lib/api';
+import type { Live } from '../lib/live.svelte';
+import type { PartRead } from '../lib/notices';
+import type { EventItem, NodeDetail, Series } from '../model/types';
+
+export type NodePart = 'detail' | 'history' | 'events';
+export const NODE_PARTS: NodePart[] = ['detail', 'history', 'events'];
+/** Each part as people read it, for the notice that says which couldn't be read. */
+export const NODE_PART_NAMES: Record<NodePart, string> = {
+  detail: 'its detail',
+  history: 'its charts',
+  events: 'its activity',
+};
+
+/** The charts cover 6 hours, a minute a point. */
+export const SPAN_S = 6 * 3600;
+export const STEP_S = 60;
+/** The node series its charts draw. */
+export const NODE_SERIES = ['lease_renew_seconds', 'memory_bytes', 'cpu_cores'] as const;
+/** The events read, and kept as they come. */
+export const EVENTS_READ = 50;
+
+export class NodePage {
+  readonly name: string;
+  detail = $state<NodeDetail | null>(null);
+  /** Each series' points, `[unix seconds, value]`, oldest first. */
+  series = $state<Record<string, [number, number][]>>({});
+  /** What the node recorded, newest first. */
+  events = $state<EventItem[]>([]);
+  read = $state<Record<NodePart, PartRead>>(
+    Object.fromEntries(NODE_PARTS.map((p) => [p, { at: null, error: null }])) as Record<NodePart, PartRead>,
+  );
+  /** No member has this name in the view of the node that answered. */
+  missing = $state(false);
+
+  #api: Client;
+  #now: () => number;
+  #at: string;
+
+  constructor(api: Client, name: string, now: () => number = Date.now) {
+    this.#api = api;
+    this.name = name;
+    this.#now = now;
+    this.#at = encodeURIComponent(name);
+  }
+
+  /** Reads `parts` (every part by default), each on its own. */
+  async load(parts: NodePart[] = NODE_PARTS): Promise<void> {
+    await Promise.all(parts.map((p) => this.#loadPart(p)));
+  }
+
+  /** Follows `live`; returns the way to stop. */
+  follow(live: Live): () => void {
+    const stops = [live.onResync(() => void this.load()), live.on('event', (d) => this.event(d as EventItem))];
+    return () => {
+      for (const stop of stops) stop();
+    };
+  }
+
+  /** An event the node recorded: added once, at the top. */
+  event(e: EventItem): void {
+    if (e.node !== this.name || this.events.some((x) => x.key === e.key)) return;
+    this.events = [e, ...this.events].slice(0, EVENTS_READ);
+  }
+
+  async #loadPart(part: NodePart): Promise<void> {
+    try {
+      switch (part) {
+        case 'detail':
+          try {
+            this.detail = await this.#api.get<NodeDetail>(`/v1/nodes/${this.#at}`);
+            this.missing = false;
+          } catch (e) {
+            if (!(e instanceof HttpError) || e.status !== 404) throw e;
+            this.missing = true;
+          }
+          break;
+        case 'history': {
+          const since = new Date(this.#now() - SPAN_S * 1000).toISOString();
+          const { series } = await this.#api.get<{ series: Series[] }>(
+            `/v1/history?node=${this.#at}&series=${NODE_SERIES.join(',')}&since=${since}&step=${STEP_S}`,
+          );
+          this.series = Object.fromEntries(series.map((s) => [s.name, s.points]));
+          break;
+        }
+        case 'events':
+          this.events = (await this.#api.get<{ events: EventItem[] }>(`/v1/events?node=${this.#at}&limit=${EVENTS_READ}`)).events;
+          break;
+      }
+      this.read[part] = { at: this.#now(), error: null };
+    } catch (e) {
+      this.read[part] = { at: this.read[part].at, error: e instanceof Error ? e.message : String(e) };
+    }
+  }
+}
````

`ui/src/nodes/view.ts`:

````diff
diff --git a/ui/src/nodes/view.ts b/ui/src/nodes/view.ts
new file mode 100644
index 0000000000000000000000000000000000000000..948f5b6d609845ae21b95ee6a022cd3deabe90ff
--- /dev/null
+++ b/ui/src/nodes/view.ts
@@ -0,0 +1,120 @@
+// A node's page (Admin_Console §4, mockup 04), worked out from what the console knows of it: the
+// header, the pipelines it holds, its gossip, and the notes heading its charts.
+
+import { stateOf } from '../cluster/derive';
+import { ago, bytes, clock, duration } from '../lib/format';
+import { formatValue } from '../lib/series';
+import { at, type Facts } from '../model/facts';
+import type { Gossip, NodeDetail, Process } from '../model/types';
+import { rows, type Tone } from '../overview/derive';
+
+export interface NodeHeader {
+  state: { label: string; tone: Tone };
+  /** `10.0.1.12:8080 · gossip 10.0.1.12:7946 · 1.1.0 · config gen 42 · started 3 d ago`. */
+  line: string;
+}
+
+/** Its header: from its own detail where read, else what the cluster's status says; `null` for a name no member has. */
+export function nodeHeader(name: string, f: Facts, d: NodeDetail | null, now: number): NodeHeader | null {
+  const m = f.cluster?.members.find((x) => x.node === name);
+  if (m === undefined && d === null) return null;
+  let state = stateOf(m ?? { suspect: false, phase: d?.phase ?? 'unknown' });
+  const unreachable = f.unreachable[name];
+  if (unreachable !== undefined) {
+    state = { label: `${state.label} · unreachable ${duration((now - unreachable) / 1000)}`, tone: 'orange' };
+  }
+  const api = (d?.api_url ?? m?.api_url ?? null)?.replace(/^https?:\/\//, '') ?? null;
+  const gossip = d?.gossip_addr ?? m?.addr ?? null;
+  const generation = d?.config_generation ?? f.config?.nodes.find((n) => n.node === name)?.generation ?? m?.config_generation ?? null;
+  const started = at(d?.started_at ?? m?.started_at);
+  const line = [
+    api,
+    gossip === null ? null : `gossip ${gossip}`,
+    d?.build ?? m?.build ?? null,
+    generation === null ? null : `config gen ${generation}`,
+    started === null ? null : `started ${ago(started, now)}`,
+  ]
+    .filter((p) => p !== null)
+    .join(' · ');
+  return { state, line };
+}
+
+export interface NodePipeline {
+  id: string;
+  href: string;
+  state: { label: string; tone: Tone };
+  lag: string;
+  /** `epoch 7 · away from owner n1`. */
+  lease: string;
+}
+
+/** The pipelines it holds, the worst first: by the cluster's leases, or its own detail before them. */
+export function nodePipelines(name: string, f: Facts, d: NodeDetail | null, now: number): NodePipeline[] {
+  const held =
+    f.cluster?.pipelines
+      .filter((l) => l.holder === name)
+      .map((l) => ({ id: l.id, owner_epoch: l.owner_epoch, ring_owner: l.ring_owner })) ??
+    d?.pipelines ??
+    [];
+  const order = rows(f, now);
+  const rank = (id: string) => {
+    const i = order.findIndex((r) => r.id === id);
+    return i === -1 ? Infinity : i;
+  };
+  return [...held]
+    .sort((a, b) => rank(a.id) - rank(b.id) || a.id.localeCompare(b.id))
+    .map((l) => {
+      const r = order.find((x) => x.id === l.id);
+      return {
+        id: l.id,
+        href: `/ui/pipelines/${encodeURIComponent(l.id)}`,
+        state: r === undefined ? { label: 'unknown', tone: 'grey' } : { label: r.state.label, tone: r.state.tone },
+        lag: r?.lag == null ? '—' : formatValue('seconds', r.lag),
+        lease: `epoch ${l.owner_epoch}${l.ring_owner !== null && l.ring_owner !== name ? ` · away from owner ${l.ring_owner}` : ''}`,
+      };
+    });
+}
+
+/** What its gossip sees: its peers, its last probe, the suspicions of the last day. */
+export function gossipLines(g: Gossip, now: number): { label: string; value: string }[] {
+  const probe = g.last_probe;
+  const probedAt = at(probe?.at);
+  const raised = g.suspicions.raised;
+  return [
+    { label: 'Sees', value: g.peers.length === 0 ? 'no peers' : g.peers.map((p) => `${p.node} ${p.state}`).join(' · ') },
+    {
+      label: 'Last probe',
+      value:
+        probe === null || probedAt === null
+          ? 'none yet'
+          : `${probe.node} ${ago(probedAt, now)}${g.probe_rtt_ms === null ? '' : ` · RTT ${g.probe_rtt_ms.toFixed(1)} ms`}`,
+    },
+    {
+      label: 'Suspicions, 24 h',
+      value: `raised ${raised.length + g.suspicions.raised_more}${
+        raised.length === 0 ? '' : ` (${raised.map((r) => `${r.node} ${clock(at(r.at) ?? now)}`).join(', ')})`
+      } · received ${g.suspicions.received + g.suspicions.received_more}`,
+    },
+  ];
+}
+
+/** The lease renewals' chart note: their slowest, and when. */
+export function renewalNote(points: [number, number][]): string {
+  if (points.length === 0) return 'no samples yet';
+  const [t, v] = points.reduce((a, b) => (b[1] > a[1] ? b : a));
+  return `peak ${formatValue('seconds', v)} at ${clock(t * 1000)}`;
+}
+
+/** The memory chart's note: in use, against its limit. */
+export function memoryNote(p: Process): string {
+  if (p.memory_bytes === null) return '—';
+  return p.memory_limit_bytes === null
+    ? `${bytes(p.memory_bytes)} · no limit`
+    : `${bytes(p.memory_bytes)} of ${bytes(p.memory_limit_bytes)} limit`;
+}
+
+/** The CPU chart's note: cores in use, against its limit. */
+export function cpuNote(p: Process): string {
+  if (p.cpu_cores === null) return '—';
+  return `${p.cpu_cores.toFixed(1)} cores · ${p.cpu_limit_cores === null ? 'no limit' : `limit ${p.cpu_limit_cores}`}`;
+}
````

- [ ] **Step 4: Run the tests and the gates**

Run `(cd ui && npm test)`: 186 passed. Then

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
git add -A ui/src/App.svelte ui/src/App.test.ts ui/src/cluster/derive.ts ui/src/model/types.ts ui/src/nodes/Node.svelte ui/src/nodes/Node.test.ts ui/src/nodes/fixtures.ts ui/src/nodes/page.svelte.ts ui/src/nodes/page.test.ts ui/src/nodes/view.test.ts ui/src/nodes/view.ts
git commit -m "feat(ui): a node's page — header, its pipelines, lease renewals, memory and CPU over 6 hours, its activity and gossip"
```

---

### Task 5: The node actions (`ui/src/nodes/actions.ts`, the dialogs)

**Files:**
- Modify: `ui/src/App.svelte`
- Modify: `ui/src/nodes/Node.svelte`
- Modify: `ui/src/nodes/Node.test.ts`
- Create: `ui/src/nodes/actions.test.ts`
- Create: `ui/src/nodes/actions.ts`

**Interfaces:**
- Consumes: M16a's `Dialog.svelte` (`title`, `lines`, `recorded`, `confirm`, `destructive`,
  `blocked`, `onConfirm` returning `null` to close or the error, `onClose`, a snippet),
  `lib/actions.ts`'s `noteBody`; Task 2's `Live.via`; Task 4's page.
- Produces (D-M16b.5): `NodeVerb = 'leave' | 'shutdown'`, `NODE_VERBS`, `NodeTarget`,
  `nodeTarget(name, f, via): NodeTarget`, `NODE_ACTIONS: Record<NodeVerb, NodeAction>` (`label`,
  `title`, `confirm`, `destructive`, `typed`, `lines`), `mayAdmin(role)`,
  `recordedOn(verb, name, session)`, `typedOk(typed, name)`, `askNode(api, verb, name, note)`;
  `Node.svelte` gains `session`, `api`, `via`, `offline`.

- [ ] **Step 1: Write the failing tests**

The dialogs' words, the role, the recorded line, the typed name, the call; on the page: an
admin's buttons, the leave sent with its note, the shutdown only once the name is typed, both
disabled offline.

`ui/src/nodes/Node.test.ts`:

````diff
diff --git a/ui/src/nodes/Node.test.ts b/ui/src/nodes/Node.test.ts
index 0afb8dd90fee1ac51cd8b5d357a5323ee2e01c35..18866bbc5d64d10795933960e44f5266ca1f81e5 100644
--- a/ui/src/nodes/Node.test.ts
+++ b/ui/src/nodes/Node.test.ts
@@ -1,6 +1,7 @@
 import { flushSync, mount, tick, unmount } from 'svelte';
 import { afterEach, describe, expect, it, vi } from 'vitest';
 import type { Client } from '../lib/api';
+import type { Role, Session } from '../lib/session';
 import { facts, NOW } from '../model/fixtures';
 import { Model } from '../model/model.svelte';
 import { LagHistory } from '../pipelines/lag.svelte';
@@ -37,14 +38,18 @@ afterEach(() => {
   mounted = undefined;
 });
 
+const sent: unknown[][] = [];
 const api: Client = {
   get: async <T,>() => ({}) as T,
-  send: async () => {
-    throw new Error('nothing is changed');
-  },
+  send: async <T,>(method: string, path: string, body?: unknown) => (sent.push([method, path, body]), {} as T),
 };
 
-async function render(over: { missing?: boolean; stale?: boolean; read?: Partial<NodePage['read']> } = {}): Promise<HTMLElement> {
+const session = (role: Role): Session => ({ name: 'Oleksii', role, auth: 'basic', expires: null, signed_in: null, csrf: 't' });
+
+async function render(
+  over: { missing?: boolean; stale?: boolean; read?: Partial<NodePage['read']>; role?: Role; offline?: boolean; via?: string } = {},
+): Promise<HTMLElement> {
+  sent.length = 0;
   const page = new NodePage(api, 'n2', () => NOW);
   page.detail = nodeDetail();
   page.series = nodeSeries();
@@ -56,7 +61,21 @@ async function render(over: { missing?: boolean; stale?: boolean; read?: Partial
   const lag = new LagHistory(api);
   const target = document.createElement('div');
   document.body.replaceChildren(target);
-  mounted = mount(Node, { target, props: { page, model, lag, now: NOW, stale: over.stale ?? false, last: NOW - 34_000 } });
+  mounted = mount(Node, {
+    target,
+    props: {
+      page,
+      model,
+      lag,
+      session: session(over.role ?? 'admin'),
+      api,
+      via: over.via ?? 'n1',
+      now: NOW,
+      stale: over.stale ?? false,
+      last: NOW - 34_000,
+      offline: over.offline ?? false,
+    },
+  });
   await vi.dynamicImportSettled();
   await tick();
   flushSync();
@@ -64,6 +83,14 @@ async function render(over: { missing?: boolean; stale?: boolean; read?: Partial
 }
 
 const text = (e: Element | null | undefined) => e?.textContent?.replace(/\s+/g, ' ').trim();
+const settle = async () => {
+  for (let i = 0; i < 5; i++) await tick();
+  flushSync();
+};
+const paragraphs = (t: HTMLElement) => [...t.querySelectorAll('[role="dialog"] p')].map((p) => text(p));
+function button(t: HTMLElement, label: string): HTMLButtonElement {
+  return [...t.querySelectorAll<HTMLButtonElement>('button')].find((b) => text(b) === label)!;
+}
 
 describe("a node's page", () => {
   it("shows mockup 04's page: its header, its pipelines, its charts, its activity and its gossip", async () => {
@@ -97,4 +124,51 @@ describe("a node's page", () => {
     expect(text(t.querySelector('.notice'))).toMatch(/^Not current: as of \d\d:\d\d:\d\d · 34 s old$/);
     expect([...t.querySelectorAll('section')].every((s) => s.classList.contains('stale'))).toBe(true);
   });
+
+  it('offers an admin Leave and Shut down, and a viewer or operator neither', async () => {
+    const buttons = (t: HTMLElement) => [...t.querySelectorAll('.actions button')].map((b) => text(b));
+    expect(buttons(await render())).toEqual(['Leave…', 'Shut down…']);
+    expect(text((await render()).querySelector('.actions + p'))).toContain('There is no in-place restart');
+    expect(buttons(await render({ role: 'operator' }))).toEqual([]);
+    expect(buttons(await render({ role: 'viewer' }))).toEqual([]);
+  });
+
+  it('leaves through its dialog: what it hands off, the note, and the line it is recorded as', async () => {
+    const t = await render();
+    button(t, 'Leave…').click();
+    flushSync();
+    const dialog = () => t.querySelector('[role="dialog"]');
+    expect(text(dialog()?.querySelector('h3'))).toBe('n2 leaves the cluster?');
+    expect(text(dialog())).toContain('Its pipelines: orders-pg.');
+    expect(paragraphs(t)).toContain('Recorded as: n2 left the cluster, asked by Oleksii (admin)');
+    const note = dialog()!.querySelector<HTMLTextAreaElement>('textarea')!;
+    note.value = 'node pool upgrade';
+    note.dispatchEvent(new Event('input'));
+    button(t, 'Leave').click();
+    await settle();
+    expect(sent).toEqual([['POST', '/v1/nodes/n2/leave', { note: 'node pool upgrade' }]]);
+    expect(dialog()).toBeNull();
+  });
+
+  it('shuts down only once its name is typed', async () => {
+    const t = await render();
+    button(t, 'Shut down…').click();
+    flushSync();
+    const confirm = button(t, 'Shut down n2');
+    expect(confirm.disabled).toBe(true);
+    expect(paragraphs(t)).toContain('Type n2 to confirm.');
+    const typed = t.querySelector<HTMLInputElement>('[role="dialog"] input.typed')!;
+    typed.value = 'n2';
+    typed.dispatchEvent(new Event('input'));
+    flushSync();
+    expect(confirm.disabled).toBe(false);
+    confirm.click();
+    await settle();
+    expect(sent).toEqual([['POST', '/v1/nodes/n2/shutdown', undefined]]);
+  });
+
+  it('waits while offline: its actions are disabled', async () => {
+    const t = await render({ offline: true });
+    expect([...t.querySelectorAll<HTMLButtonElement>('.actions button')].map((b) => b.disabled)).toEqual([true, true]);
+  });
 });
````

`ui/src/nodes/actions.test.ts`:

````diff
diff --git a/ui/src/nodes/actions.test.ts b/ui/src/nodes/actions.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..49591d20feb9e2f5aacde34908023e38f419d141
--- /dev/null
+++ b/ui/src/nodes/actions.test.ts
@@ -0,0 +1,71 @@
+import { describe, expect, it } from 'vitest';
+import type { Client } from '../lib/api';
+import { facts } from '../model/fixtures';
+import { askNode, mayAdmin, NODE_ACTIONS, nodeTarget, recordedOn, typedOk } from './actions';
+
+describe("a node's actions", () => {
+  it('are an admin\'s only', () => {
+    expect([mayAdmin('viewer'), mayAdmin('operator'), mayAdmin('admin')]).toEqual([false, false, true]);
+  });
+
+  it('say what a leave hands off, where a running snapshot goes on, and how long it takes', () => {
+    const t = nodeTarget('n1', facts(), 'n2');
+    expect(t).toEqual({ name: 'n1', pipelines: ['billing-mysql', 'customers'], snapshots: [{ job: '019ab5d0', pipeline: 'customers' }], members: 3, via: 'n2', drainSeconds: 60 });
+    expect(NODE_ACTIONS.leave.title('n1')).toBe('n1 leaves the cluster?');
+    expect(NODE_ACTIONS.leave.lines(t)).toEqual([
+      'n1 hands its 2 pipelines to the others, one at a time, and leaves the ring. The process keeps running, out of the cluster, until it is stopped or restarted.',
+      'Its pipelines: billing-mysql, customers. The ring places each on another node.',
+      'Its running snapshot 019ab5d0 of customers goes on from its last chunk where customers goes.',
+      "Each hand-off pauses its pipeline for a while; lag rises meanwhile. It ends when every hand-off is done, or after drain_timeout (60 s) for each round.",
+    ]);
+  });
+
+  it("says a shutdown leaves first, then exits, and who carries its pipelines meanwhile", () => {
+    const t = nodeTarget('n2', facts(), 'n1');
+    expect(NODE_ACTIONS.shutdown.title('n2')).toBe('Shut down n2?');
+    expect(NODE_ACTIONS.shutdown.lines(t)).toEqual([
+      'n2 leaves the cluster as a leave does, handing off its 1 pipeline, then exits. A supervisor such as Kubernetes starts it again, and it rejoins; without one, nothing restarts it.',
+      'With 3 nodes, the other 2 carry everything until it is back.',
+    ]);
+    expect([NODE_ACTIONS.leave.destructive, NODE_ACTIONS.shutdown.destructive]).toEqual([true, true]);
+    expect([NODE_ACTIONS.leave.typed, NODE_ACTIONS.shutdown.typed]).toEqual([false, true]);
+  });
+
+  it("says so when the node is the one the console's live updates come through", () => {
+    const t = nodeTarget('n2', facts(), 'n2');
+    expect(NODE_ACTIONS.leave.lines(t).at(-1)).toBe(
+      "This console's live updates come through n2: once it goes, the page reconnects to another node on its own.",
+    );
+    expect(NODE_ACTIONS.shutdown.lines(t).at(-1)).toBe(NODE_ACTIONS.leave.lines(t).at(-1));
+  });
+
+  it('says a node holding nothing just leaves the ring', () => {
+    const t = nodeTarget('n9', facts(), 'n1');
+    expect(NODE_ACTIONS.leave.lines(t)[0]).toBe(
+      'n9 holds no pipeline: it leaves the ring. The process keeps running, out of the cluster, until it is stopped or restarted.',
+    );
+  });
+
+  it('is recorded under the name of who asked, and needs the name typed to shut down', () => {
+    const s = { name: 'Oleksii', role: 'admin' as const };
+    expect(recordedOn('leave', 'n2', s)).toBe('n2 left the cluster, asked by Oleksii (admin)');
+    expect(recordedOn('shutdown', 'n2', s)).toBe('n2 shut down by Oleksii (admin)');
+    expect([typedOk('n2', 'n2'), typedOk(' n2 ', 'n2'), typedOk('n', 'n2'), typedOk('N2', 'n2')]).toEqual([true, true, false, false]);
+  });
+
+  it("asks the node through any node, with the note", async () => {
+    const sent: unknown[][] = [];
+    const api: Client = {
+      get: async () => {
+        throw new Error('nothing is read');
+      },
+      send: async <T,>(method: string, path: string, body?: unknown) => (sent.push([method, path, body]), {} as T),
+    };
+    await askNode(api, 'shutdown', 'node b', ' kernel patch ');
+    await askNode(api, 'leave', 'n2', '');
+    expect(sent).toEqual([
+      ['POST', '/v1/nodes/node%20b/shutdown', { note: 'kernel patch' }],
+      ['POST', '/v1/nodes/n2/leave', undefined],
+    ]);
+  });
+});
````

- [ ] **Step 2: Run them to see them fail**

Run: `(cd ui && npm test)`
Expected: 4 failed, 186 passed, and `nodes/actions.test.ts` can't load (`./actions` isn't
there): `Node.test.ts`'s four action tests (no `.actions`).

- [ ] **Step 3: Implement**

`actions.ts`; the page's buttons, their explanation and the dialog (the typed name a
child input of the shutdown's); `App.svelte` passes the session, the client, `live.via` and
whether it is offline.

`ui/src/App.svelte`:

````diff
diff --git a/ui/src/App.svelte b/ui/src/App.svelte
index aca6aba0ac305cae8e0c92d9d488527452c0689d..c58741f45a5f4bb31cbb620a780a6c33b7b6127f 100644
--- a/ui/src/App.svelte
+++ b/ui/src/App.svelte
@@ -134,7 +134,18 @@
       <Cluster {model} {renewals} {now} stale={live.stale(now)} last={live.last} />
     {:else if nodePage !== null && nodePage.name === nodeName}
       {#key nodePage}
-        <Node page={nodePage} {model} {lag} {now} stale={live.stale(now)} last={live.last} />
+        <Node
+          page={nodePage}
+          {model}
+          {lag}
+          {session}
+          {api}
+          via={live.via}
+          {now}
+          stale={live.stale(now)}
+          last={live.last}
+          offline={live.state === 'offline'}
+        />
       {/key}
     {:else if page !== null && page.id === pageId}
       <!-- Each pipeline's page starts clean: nothing chosen or open carries over from the last. -->
````

`ui/src/nodes/Node.svelte`:

````diff
diff --git a/ui/src/nodes/Node.svelte b/ui/src/nodes/Node.svelte
index fdda520f24a3a0fa5a88b23000a15451ae9f0051..0cda4e29637105762e7b3bcd75e2e2f70d020d77 100644
--- a/ui/src/nodes/Node.svelte
+++ b/ui/src/nodes/Node.svelte
@@ -1,14 +1,19 @@
 <script lang="ts">
   // A node's page (Admin_Console §4, mockup 04): its header, the pipelines it holds, its lease
-  // renewals, memory and CPU over 6 hours, what it recorded, and what its gossip sees.
+  // renewals, memory and CPU over 6 hours, what it recorded, and what its gossip sees; an admin's
+  // leave and shutdown (mockup 11).
   import { onMount } from 'svelte';
+  import type { Client } from '../lib/api';
   import Chart from '../lib/Chart.svelte';
+  import Dialog from '../lib/Dialog.svelte';
   import { failingNotice, failures, staleNotice, type PartRead } from '../lib/notices';
+  import type { Session } from '../lib/session';
   import type { Model } from '../model/model.svelte';
   import { LAG_TARGET_S } from '../overview/attention';
   import Sparkline from '../overview/Sparkline.svelte';
   import { activity } from '../pipelines/detail';
   import type { LagHistory } from '../pipelines/lag.svelte';
+  import { askNode, mayAdmin, NODE_ACTIONS, NODE_VERBS, nodeTarget, recordedOn, typedOk, type NodeVerb } from './actions';
   import { NODE_PART_NAMES, NODE_PARTS, SPAN_S, type NodePage } from './page.svelte';
   import { cpuNote, gossipLines, memoryNote, nodeHeader, nodePipelines, renewalNote } from './view';
 
@@ -17,12 +22,18 @@
     model: Model;
     /** Every pipeline's lag over 6 hours. */
     lag: LagHistory;
+    session: Session;
+    api: Client;
+    /** The node the console's live updates come through. */
+    via: string | null;
     now: number;
     /** The live updates stopped: every number is as of `last`. */
     stale: boolean;
     last: number | null;
+    /** Offline: actions wait. */
+    offline: boolean;
   }
-  let { page, model, lag, now, stale, last }: Props = $props();
+  let { page, model, lag, session, api, via, now, stale, last, offline }: Props = $props();
 
   const name = $derived(page.name);
   const head = $derived(nodeHeader(name, model.facts, page.detail, now));
@@ -36,6 +47,33 @@
   );
   const series = (s: string) => page.series[s] ?? [];
 
+  /** A member still in the ring: a leaving one is leaving already. */
+  const active = $derived(model.facts.cluster?.members.find((m) => m.node === name)?.phase === 'active');
+  let acting = $state<NodeVerb | null>(null);
+  let typed = $state('');
+  const blocked = $derived(
+    offline
+      ? 'Offline: actions wait until the console is live again.'
+      : acting !== null && NODE_ACTIONS[acting].typed && !typedOk(typed, name)
+        ? `Type ${name} to confirm.`
+        : null,
+  );
+
+  function open(v: NodeVerb) {
+    typed = '';
+    acting = v;
+  }
+
+  async function act(v: NodeVerb, note: string): Promise<string | null> {
+    try {
+      // What follows comes on the stream: the recorded action, and the member's new phase.
+      await askNode(api, v, name, note);
+      return null;
+    } catch (e) {
+      return e instanceof Error ? e.message : String(e);
+    }
+  }
+
   onMount(() => void lag.ensure());
 </script>
 
@@ -49,6 +87,20 @@
     {#if head !== null}<span class="chip {head.state.tone}">{head.state.label}</span>{/if}
   </h2>
   {#if head !== null}<p class="where muted">{head.line}</p>{/if}
+  {#if mayAdmin(session.role) && head !== null}
+    <div class="actions">
+      {#each NODE_VERBS as v (v)}
+        <button type="button" class="destructive" disabled={offline || !active} onclick={() => open(v)}>
+          {NODE_ACTIONS[v].label}…
+        </button>
+      {/each}
+    </div>
+    <p class="muted">
+      Leave hands its pipelines to the others and stays up, out of the ring. Shut down leaves, then
+      exits; a supervisor such as Kubernetes starts it again. There is no in-place restart. Admin
+      only.
+    </p>
+  {/if}
   {#if stale}
     <p class="notice" role="status">{staleNotice(last, now)}</p>
   {/if}
@@ -143,12 +195,38 @@
       {/if}
     </section>
   </div>
+
+  {#if acting !== null}
+    {@const v = acting}
+    {@const target = nodeTarget(name, model.facts, via)}
+    <Dialog
+      title={NODE_ACTIONS[v].title(name)}
+      lines={NODE_ACTIONS[v].lines(target)}
+      recorded={recordedOn(v, name, session)}
+      confirm={NODE_ACTIONS[v].confirm(name)}
+      destructive={NODE_ACTIONS[v].destructive}
+      {blocked}
+      onConfirm={(note) => act(v, note)}
+      onClose={() => (acting = null)}
+    >
+      {#if NODE_ACTIONS[v].typed}
+        <label>Type {name} to confirm <input class="typed" bind:value={typed} autocomplete="off" spellcheck="false" /></label>
+      {/if}
+    </Dialog>
+  {/if}
 {/if}
 
 <style>
   .where {
     margin-top: -6px;
   }
+  .actions {
+    display: flex;
+    gap: 8px;
+  }
+  .destructive {
+    color: var(--red);
+  }
   .notice {
     padding: 6px 10px;
     border-radius: 6px;
````

`ui/src/nodes/actions.ts`:

````diff
diff --git a/ui/src/nodes/actions.ts b/ui/src/nodes/actions.ts
new file mode 100644
index 0000000000000000000000000000000000000000..fa80b46c1e53e8b7a0f6a8093e7a5cca8de4ae5e
--- /dev/null
+++ b/ui/src/nodes/actions.ts
@@ -0,0 +1,129 @@
+// A node's two actions (Admin_Console §5, mockup 11): leave the cluster, or shut down. An admin's
+// only; sent through whichever node serves the page, the target checking the caller again and
+// recording it. Neither can be undone from the console, so both are styled as destructive, and
+// a shutdown needs the node's name typed.
+
+import type { Client } from '../lib/api';
+import { noteBody } from '../lib/actions';
+import type { Role, Session } from '../lib/session';
+import type { Facts } from '../model/facts';
+import { shortJob } from '../overview/attention';
+
+export type NodeVerb = 'leave' | 'shutdown';
+export const NODE_VERBS: NodeVerb[] = ['leave', 'shutdown'];
+
+/** What a node's dialog says it acts on. */
+export interface NodeTarget {
+  name: string;
+  /** The pipelines whose lease it holds. */
+  pipelines: string[];
+  /** Their running snapshot jobs, by short id. */
+  snapshots: { job: string; pipeline: string }[];
+  /** How many members the cluster has, it among them. */
+  members: number;
+  /** The node the console's live updates come through. */
+  via: string | null;
+  drainSeconds: number | null;
+}
+
+export function nodeTarget(name: string, f: Facts, via: string | null): NodeTarget {
+  const pipelines = (f.cluster?.pipelines ?? [])
+    .filter((l) => l.holder === name)
+    .map((l) => l.id)
+    .sort();
+  return {
+    name,
+    pipelines,
+    snapshots: f.jobs
+      .filter((j) => j.status === 'running' && pipelines.includes(j.pipeline))
+      .map((j) => ({ job: shortJob(j.job_id), pipeline: j.pipeline })),
+    members: f.cluster?.members.length ?? 0,
+    via,
+    drainSeconds: f.cluster?.settings?.drain_timeout_seconds ?? null,
+  };
+}
+
+const plural = (n: number, one: string) => `${n} ${one}${n === 1 ? '' : 's'}`;
+const STAYS_UP = 'The process keeps running, out of the cluster, until it is stopped or restarted.';
+
+/** The line when the console's live updates come through the node. */
+function viaLine(t: NodeTarget): string[] {
+  return t.via === t.name
+    ? [`This console's live updates come through ${t.name}: once it goes, the page reconnects to another node on its own.`]
+    : [];
+}
+
+export interface NodeAction {
+  label: string;
+  title: (name: string) => string;
+  confirm: (name: string) => string;
+  destructive: boolean;
+  /** Confirmed only once the node's name is typed. */
+  typed: boolean;
+  lines: (t: NodeTarget) => string[];
+}
+
+export const NODE_ACTIONS: Record<NodeVerb, NodeAction> = {
+  leave: {
+    label: 'Leave',
+    title: (n) => `${n} leaves the cluster?`,
+    confirm: () => 'Leave',
+    destructive: true,
+    typed: false,
+    lines: (t) => {
+      const k = t.pipelines.length;
+      if (k === 0) return [`${t.name} holds no pipeline: it leaves the ring. ${STAYS_UP}`, ...viaLine(t)];
+      return [
+        `${t.name} hands its ${plural(k, 'pipeline')} to the others, one at a time, and leaves the ring. ${STAYS_UP}`,
+        `Its pipelines: ${t.pipelines.join(', ')}. The ring places each on another node.`,
+        ...t.snapshots.map(
+          (s) => `Its running snapshot ${s.job} of ${s.pipeline} goes on from its last chunk where ${s.pipeline} goes.`,
+        ),
+        `Each hand-off pauses its pipeline for a while; lag rises meanwhile. It ends when every hand-off is done, or after drain_timeout${
+          t.drainSeconds === null ? '' : ` (${t.drainSeconds} s)`
+        } for each round.`,
+        ...viaLine(t),
+      ];
+    },
+  },
+  shutdown: {
+    label: 'Shut down',
+    title: (n) => `Shut down ${n}?`,
+    confirm: (n) => `Shut down ${n}`,
+    destructive: true,
+    typed: true,
+    lines: (t) => {
+      const k = t.pipelines.length;
+      const others = t.members - 1;
+      return [
+        `${t.name} leaves the cluster as a leave does${k === 0 ? '' : `, handing off its ${plural(k, 'pipeline')}`}, then exits. A supervisor such as Kubernetes starts it again, and it rejoins; without one, nothing restarts it.`,
+        others > 0
+          ? `With ${t.members} nodes, the other ${others} carry everything until it is back.`
+          : 'It is the only node: nothing runs its pipelines until it is back.',
+        ...viaLine(t),
+      ];
+    },
+  },
+};
+
+/** Whether `role` may act on nodes: an admin's only. */
+export function mayAdmin(role: Role): boolean {
+  return role === 'admin';
+}
+
+/** The line a node's action will be recorded as. */
+export function recordedOn(verb: NodeVerb, name: string, s: Pick<Session, 'name' | 'role'>): string {
+  return verb === 'leave'
+    ? `${name} left the cluster, asked by ${s.name} (${s.role})`
+    : `${name} shut down by ${s.name} (${s.role})`;
+}
+
+/** Whether what was typed names the node. */
+export function typedOk(typed: string, name: string): boolean {
+  return typed.trim() === name;
+}
+
+/** Asks `name` to leave or shut down, through any node, with `note`. */
+export async function askNode(api: Client, verb: NodeVerb, name: string, note: string): Promise<void> {
+  await api.send('POST', `/v1/nodes/${encodeURIComponent(name)}/${verb}`, noteBody(note));
+}
````

- [ ] **Step 4: Run the tests and the gates**

Run `(cd ui && npm test)`: 197 passed. Then

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
git add -A ui/src/App.svelte ui/src/nodes/Node.svelte ui/src/nodes/Node.test.ts ui/src/nodes/actions.test.ts ui/src/nodes/actions.ts
git commit -m "feat(ui): a node's leave and shutdown, an admin's, through their dialogs; a shutdown confirmed by the node's name"
```

---

### Task 6: The console's status (`ui/src/lib/status.ts`, the user menu)

**Files:**
- Modify: `ui/src/App.svelte`
- Modify: `ui/src/Frame.svelte`
- Modify: `ui/src/Frame.test.ts`
- Create: `ui/src/lib/status.test.ts`
- Create: `ui/src/lib/status.ts`
- Modify: `ui/src/model/facts.ts`
- Modify: `ui/src/model/fixtures.ts`
- Modify: `ui/src/model/model.svelte.ts`
- Modify: `ui/src/model/model.test.ts`
- Modify: `ui/src/model/types.ts`

**Interfaces:**
- Consumes: Task 1's `settings`; Task 2's `Live.via`, `facts.unreachable`; M13e's
  `prometheus` in a `/v1/history` answer.
- Produces (D-M16b.6): `types.ts`'s `PrometheusStatus { state: 'unasked' | 'ok' | 'failing'; at?;
  error? }`; `Facts.prometheus: PrometheusStatus | null`, `Facts.lastSample: number | null`;
  `status.ts`'s `StatusTone`, `StatusRow { label; chip; text; tone }`, `StatusInput { state; via;
  last; facts; read }`, `retention(seconds)`, `consoleStatus(i, now): StatusRow[]`; `Frame.svelte`
  gains `status` and `onStatus`.

- [ ] **Step 1: Write the failing tests**

Each row's words in each state; the model keeps Prometheus's state from the lag lines'
read and the newest sample's time; the menu draws its table only while open and reads again as
it opens.

`ui/src/Frame.test.ts`:

````diff
diff --git a/ui/src/Frame.test.ts b/ui/src/Frame.test.ts
index 53f367217098bf906cdc776177b092e68a1c6866..eff4c5e136a09138dd3decfce64ce6a1dc6b16eb 100644
--- a/ui/src/Frame.test.ts
+++ b/ui/src/Frame.test.ts
@@ -32,6 +32,8 @@ function render(props: Partial<Parameters<typeof Frame>[1]> = {}): HTMLElement {
       last: NOW,
       now: NOW,
       onRetry: () => {},
+      status: [],
+      onStatus: () => {},
       ...props,
     },
   });
@@ -61,6 +63,39 @@ describe('the frame', () => {
     );
   });
 
+  it("shows the console's status from the user menu, read again as it opens", () => {
+    const onStatus = vi.fn();
+    const page = render({
+      onStatus,
+      status: [
+        { label: 'Live updates', chip: 'connected', text: 'via n1', tone: 'ok' },
+        { label: 'Prometheus', chip: 'failing', text: 'connection refused · last asked 13:40', tone: 'red' },
+        { label: 'Event store', chip: null, text: 'not read yet', tone: 'muted' },
+      ],
+    });
+    const menu = page.querySelector<HTMLDetailsElement>('.who details.menu');
+    expect(menu?.querySelector('summary')?.textContent).toMatch(/^Oleksii\s+operator$/);
+    // Closed, it draws nothing: its rows aren't the page's.
+    expect(menu?.querySelector('.status')).toBeNull();
+    menu!.open = true;
+    menu!.dispatchEvent(new Event('toggle'));
+    flushSync();
+    expect(menu?.querySelector('h3')?.textContent).toBe('Console status');
+    const rows = [...(menu?.querySelectorAll('tr') ?? [])].map((r) => [...r.querySelectorAll('td')].map((d) => d.textContent?.trim()));
+    expect(rows).toEqual([
+      ['Live updates', 'connected via n1'],
+      ['Prometheus', 'failing connection refused · last asked 13:40'],
+      ['Event store', 'not read yet'],
+    ]);
+    expect(menu?.querySelector('.chip.red')?.textContent).toBe('failing');
+    expect(menu?.querySelector('td.muted')?.textContent?.trim()).toBe('not read yet');
+    menu!.open = false;
+    menu!.dispatchEvent(new Event('toggle'));
+    flushSync();
+    expect(menu?.querySelector('.status')).toBeNull();
+    expect(onStatus).toHaveBeenCalledOnce();
+  });
+
   it('says how old the page is once the updates stop', () => {
     const late = render({ state: 'reconnecting', now: NOW + 14_000 });
     expect(late.querySelector('[role="status"]')?.textContent).toBe('reconnecting · last update 14 s ago');
````

`ui/src/lib/status.test.ts`:

````diff
diff --git a/ui/src/lib/status.test.ts b/ui/src/lib/status.test.ts
new file mode 100644
index 0000000000000000000000000000000000000000..e5da703db931da26dfc2484a7c2012d04a6c1767
--- /dev/null
+++ b/ui/src/lib/status.test.ts
@@ -0,0 +1,101 @@
+import { describe, expect, it } from 'vitest';
+import { facts, iso, NOW } from '../model/fixtures';
+import { PARTS, type Part, type Read } from '../model/model.svelte';
+import { consoleStatus, retention, type StatusInput } from './status';
+
+const ASKED = new Date(2026, 10, 25, 13, 40).getTime();
+
+function reads(over: Partial<Record<Part, Read>> = {}): Record<Part, Read> {
+  return { ...(Object.fromEntries(PARTS.map((p) => [p, { at: NOW - 1_000, error: null }])) as Record<Part, Read>), ...over };
+}
+
+function input(over: Partial<StatusInput> = {}): StatusInput {
+  const f = facts();
+  f.unreachable = { n3: NOW - 40_000 };
+  f.lastSample = NOW - 6_000;
+  f.prometheus = { state: 'failing', at: iso(ASKED), error: 'connection refused' };
+  return { state: 'live', via: 'n1', last: NOW - 2_000, facts: f, read: reads(), ...over };
+}
+
+describe("the console's status", () => {
+  it('says how the live updates, the nodes, the history, Prometheus and the event store are', () => {
+    expect(consoleStatus(input(), NOW)).toEqual([
+      { label: 'Live updates', chip: 'connected', text: 'via n1', tone: 'ok' },
+      { label: 'Nodes answering', chip: null, text: '2 of 3 · n3 40 s', tone: 'yellow' },
+      { label: 'Built-in history', chip: null, text: '24 h · last sample 6 s ago', tone: null },
+      { label: 'Prometheus', chip: 'failing', text: 'connection refused · last asked 13:40', tone: 'red' },
+      { label: 'Event store', chip: 'ok', text: '30 days', tone: 'ok' },
+    ]);
+  });
+
+  it('says when the live updates stopped, and since when', () => {
+    const row = (i: Partial<StatusInput>) => consoleStatus(input(i), NOW)[0];
+    expect(row({ state: 'reconnecting', last: NOW - 14_000 })).toEqual({
+      label: 'Live updates',
+      chip: 'reconnecting',
+      text: 'last update 14 s ago',
+      tone: 'yellow',
+    });
+    expect(row({ state: 'offline', last: NOW - 120_000 })).toEqual({
+      label: 'Live updates',
+      chip: 'offline',
+      text: 'last update 2 min ago',
+      tone: 'red',
+    });
+    expect(row({ state: 'connecting', last: null })).toEqual({ label: 'Live updates', chip: 'connecting', text: '', tone: null });
+  });
+
+  it('says what it has not read yet, and what is not configured', () => {
+    const f = facts();
+    f.cluster = null;
+    f.prometheus = null;
+    const unread = consoleStatus(
+      input({ facts: f, read: reads({ lag: { at: null, error: null }, events: { at: null, error: null } }) }),
+      NOW,
+    );
+    expect(unread.map((r) => [r.label, r.text, r.tone])).toEqual([
+      ['Live updates', 'via n1', 'ok'],
+      ['Nodes answering', 'not read yet', 'muted'],
+      ['Built-in history', 'no sample yet', 'muted'],
+      ['Prometheus', 'not read yet', 'muted'],
+      ['Event store', 'not read yet', 'muted'],
+    ]);
+    const plain = consoleStatus(input({ facts: { ...facts(), prometheus: null, lastSample: null } }), NOW);
+    expect(plain[1]).toEqual({ label: 'Nodes answering', chip: null, text: '3 of 3', tone: null });
+    expect(plain[2].text).toBe('24 h · no sample yet');
+    expect(plain[3]).toEqual({ label: 'Prometheus', chip: null, text: 'not configured: charts show the built-in history only', tone: 'muted' });
+  });
+
+  it("says how Prometheus answered when it did, or that no chart has asked it yet", () => {
+    const prom = (p: NonNullable<StatusInput['facts']['prometheus']>) =>
+      consoleStatus(input({ facts: { ...facts(), prometheus: p } }), NOW)[3];
+    expect(prom({ state: 'ok', at: iso(ASKED), error: null })).toEqual({
+      label: 'Prometheus',
+      chip: 'ok',
+      text: 'last asked 13:40',
+      tone: 'ok',
+    });
+    expect(prom({ state: 'unasked' })).toEqual({
+      label: 'Prometheus',
+      chip: null,
+      text: 'configured; no chart has reached past the built-in history yet',
+      tone: 'muted',
+    });
+  });
+
+  it("says why the event store can't be read", () => {
+    const rows = consoleStatus(input({ read: reads({ events: { at: NOW - 60_000, error: 'the request timed out' } }) }), NOW);
+    expect(rows[4]).toEqual({ label: 'Event store', chip: 'failing', text: 'the request timed out', tone: 'red' });
+  });
+
+  it('writes a retention in days or hours', () => {
+    expect([86_400, 2_592_000, 604_800, 172_800, 3_600, 5_400].map(retention)).toEqual([
+      '24 h',
+      '30 days',
+      '7 days',
+      '2 days',
+      '1 h',
+      '1h 30m',
+    ]);
+  });
+});
````

`ui/src/model/model.test.ts`:

````diff
diff --git a/ui/src/model/model.test.ts b/ui/src/model/model.test.ts
index 3aec9b7cab4a4a5ffbef43272c481005459a6b50..58ff98f0d6b9f87aebd071239d4dd264c66b4131 100644
--- a/ui/src/model/model.test.ts
+++ b/ui/src/model/model.test.ts
@@ -207,6 +207,27 @@ describe("the console's model", () => {
     expect(a.asked).toEqual(expect.arrayContaining(['/v1/config/nodes', '/v1/cluster/removals']));
   });
 
+  it("keeps how Prometheus last answered the node, from the lag lines' read, and when the last sample came", async () => {
+    const o = new Model(
+      api({
+        ...READS,
+        '/v1/history?series=lag_seconds': {
+          series: [],
+          prometheus: { state: 'failing', at: iso(NOW - 60_000), error: 'connection refused' },
+        },
+      }),
+    );
+    await o.load(['lag']);
+    expect(o.facts.prometheus).toEqual({ state: 'failing', at: iso(NOW - 60_000), error: 'connection refused' });
+    const plain = new Model(api(READS));
+    await plain.load(['lag']);
+    expect(plain.facts.prometheus).toBeNull();
+    expect(plain.facts.lastSample).toBeNull();
+    plain.samples({ scope: 'node', at: NOW / 1000 - 6, series: { memory_bytes: 1 } });
+    plain.samples({ scope: 'pipeline', pipeline: 'orders-pg', at: NOW / 1000 - 20, series: { lag_seconds: 3 } });
+    expect(plain.facts.lastSample).toBe(NOW - 6_000);
+  });
+
   it('names its parts as people read them, for the notices that say which failed', () => {
     expect(PART_NAMES).toEqual({
       pipelines: 'the pipelines',
````

- [ ] **Step 2: Run them to see them fail**

Run: `(cd ui && npm test)`
Expected: 2 failed, 197 passed, and `lib/status.test.ts` can't load: `Frame.test.ts`'s menu (no
`<summary>`), `model.test.ts`'s Prometheus state (`undefined`).

- [ ] **Step 3: Implement**

`status.ts`; the model's two facts; the menu in `Frame.svelte` (its local `state` prop is
named `conn` there, beside the `$state` rune); `App.svelte` passes the rows and rereads the lag
lines and the events when the menu opens.

`ui/src/App.svelte`:

````diff
diff --git a/ui/src/App.svelte b/ui/src/App.svelte
index c58741f45a5f4bb31cbb620a780a6c33b7b6127f..52fd23b7894f34ae4c1ffe2451f602edfa66b735 100644
--- a/ui/src/App.svelte
+++ b/ui/src/App.svelte
@@ -8,6 +8,7 @@
   import { client } from './lib/api';
   import { Live } from './lib/live.svelte';
   import { Router } from './lib/router.svelte';
+  import { consoleStatus } from './lib/status';
   import { awaitSession, checkSession, keepFresh, RETRY_MS, type Session } from './lib/session';
   import { Model } from './model/model.svelte';
   import Node from './nodes/Node.svelte';
@@ -127,6 +128,8 @@
     last={live.last}
     {now}
     onRetry={() => live.retry()}
+    status={consoleStatus({ state: live.state, via: live.via, last: live.last, facts: model.facts, read: model.read }, now)}
+    onStatus={() => void model.load(['lag', 'events'])}
   >
     {#if router.section === 'overview'}
       <Overview {model} {now} stale={live.stale(now)} last={live.last} />
````

`ui/src/Frame.svelte`:

````diff
diff --git a/ui/src/Frame.svelte b/ui/src/Frame.svelte
index f2c6072d2b180959d06daec8b0b2f717a82bad56..bc51adb0960b140ea8066fbc997a4c54dbc53bb6 100644
--- a/ui/src/Frame.svelte
+++ b/ui/src/Frame.svelte
@@ -1,11 +1,13 @@
 <script lang="ts">
   // The top bar every page shares (Admin_Console §4; mockups 01 and 11): the sections, whether
-  // the page is live, the config generation, and who is signed in with their role.
+  // the page is live, the config generation, and who is signed in with their role; the user menu
+  // holds the console's own status.
   import type { Snippet } from 'svelte';
   import { clock } from './lib/format';
   import type { State } from './lib/live.svelte';
   import { SECTIONS, type Section } from './lib/router.svelte';
   import type { Session } from './lib/session';
+  import type { StatusRow } from './lib/status';
 
   interface Props {
     session: Session;
@@ -16,15 +18,22 @@
     last: number | null;
     now: number;
     onRetry: () => void;
+    /** The console's status, shown from the user menu. */
+    status: StatusRow[];
+    /** The user menu opened: what the status reads is read again. */
+    onStatus: () => void;
     children?: Snippet;
   }
-  let { session, section, generation, state, last, now, onRetry, children }: Props = $props();
+  let { session, section, generation, state: conn, last, now, onRetry, status, onStatus, children }: Props = $props();
+
+  /** The user menu is open: its status is drawn only then. */
+  let open = $state(false);
 
   const connection = $derived.by(() => {
-    if (state === 'reconnecting' && last !== null) {
+    if (conn === 'reconnecting' && last !== null) {
       return `reconnecting · last update ${Math.floor((now - last) / 1000)} s ago`;
     }
-    return state;
+    return conn;
   });
 </script>
 
@@ -36,10 +45,29 @@
     {/each}
   </nav>
   <div class="who">
-    <span class="conn {state}" role="status">{connection}</span>
+    <span class="conn {conn}" role="status">{connection}</span>
     {#if generation !== null}<span>config gen {generation}</span>{/if}
-    <span>{session.name}</span>
-    <b>{session.role}</b>
+    <details class="menu" bind:open ontoggle={() => open && onStatus()}>
+      <summary>{session.name} <b>{session.role}</b></summary>
+      {#if open}
+        <div class="status">
+          <h3>Console status</h3>
+          <table>
+            <tbody>
+              {#each status as r (r.label)}
+                <tr>
+                  <td>{r.label}</td>
+                  <td class={r.tone === 'muted' || r.tone === 'red' ? r.tone : undefined}>
+                    {#if r.chip !== null}<span class="chip {r.tone}">{r.chip}</span>{/if}
+                    {r.text}
+                  </td>
+                </tr>
+              {/each}
+            </tbody>
+          </table>
+        </div>
+      {/if}
+    </details>
     {#if session.csrf !== null}
       <form method="post" action="/ui/sign-out"><button type="submit">Sign out</button></form>
     {/if}
@@ -50,7 +78,7 @@
     This cluster has no authentication: everyone who reaches the console is admin.
   </div>
 {/if}
-{#if state === 'offline'}
+{#if conn === 'offline'}
   <div class="down" role="alert">
     {#if last !== null}Offline since {clock(last, true)} — everything below is as of then.{:else}Offline:
       the console can't reach the cluster's live updates.{/if}
@@ -116,6 +144,61 @@
   form {
     display: inline;
   }
+  .menu {
+    position: relative;
+  }
+  .menu summary {
+    cursor: pointer;
+    list-style: none;
+  }
+  .menu summary b {
+    color: var(--text);
+  }
+  .status {
+    position: absolute;
+    right: 0;
+    top: calc(100% + 8px);
+    z-index: 10;
+    min-width: 340px;
+    padding: 10px 12px;
+    background: var(--bg-panel);
+    border: 1px solid var(--border);
+    border-radius: 6px;
+    color: var(--text);
+  }
+  .status h3 {
+    margin: 0 0 6px;
+    font-size: 13px;
+  }
+  .status td {
+    padding: 3px 8px 3px 0;
+    vertical-align: top;
+  }
+  .status td:first-child {
+    color: var(--text-2);
+    white-space: nowrap;
+  }
+  .status td.red {
+    color: var(--red);
+  }
+  .status td.muted {
+    color: var(--text-2);
+  }
+  .chip {
+    display: inline-block;
+    padding: 0 6px;
+    border-radius: 8px;
+    background: var(--grey-bg);
+  }
+  .chip.ok {
+    background: var(--green-bg);
+  }
+  .chip.yellow {
+    background: var(--yellow-bg);
+  }
+  .chip.red {
+    background: var(--red-bg);
+  }
   .banner,
   .down {
     padding: 8px 16px;
````

`ui/src/lib/status.ts`:

````diff
diff --git a/ui/src/lib/status.ts b/ui/src/lib/status.ts
new file mode 100644
index 0000000000000000000000000000000000000000..e2527340d4afd99d465a907e6f3ba72f97081fd6
--- /dev/null
+++ b/ui/src/lib/status.ts
@@ -0,0 +1,111 @@
+// The console's own status (Admin_Console §7, §8; mockup 11), shown from the user menu: whether
+// the live updates come, how many nodes answer, the built-in history, Prometheus and the event
+// store.
+
+import { at, type Facts } from '../model/facts';
+import type { Part, Read } from '../model/model.svelte';
+import { ago, clock, duration } from './format';
+import type { State } from './live.svelte';
+
+export type StatusTone = 'ok' | 'yellow' | 'red' | 'muted' | null;
+
+export interface StatusRow {
+  label: string;
+  /** A short state shown as a chip, before the text. */
+  chip: string | null;
+  text: string;
+  tone: StatusTone;
+}
+
+export interface StatusInput {
+  state: State;
+  /** The node the live updates come through. */
+  via: string | null;
+  /** When the last message came (ms). */
+  last: number | null;
+  facts: Facts;
+  read: Record<Part, Read>;
+}
+
+/** A retention as people say it: `24 h`, `30 days`, else as a duration. */
+export function retention(seconds: number): string {
+  if (seconds >= 2 * 86_400 && seconds % 86_400 === 0) return `${seconds / 86_400} days`;
+  if (seconds % 3_600 === 0) return `${seconds / 3_600} h`;
+  return duration(seconds);
+}
+
+const row = (label: string, chip: string | null, text: string, tone: StatusTone): StatusRow => ({
+  label,
+  chip,
+  text,
+  tone,
+});
+
+function liveRow(i: StatusInput, now: number): StatusRow {
+  const since = i.last === null ? '' : `last update ${ago(i.last, now)}`;
+  switch (i.state) {
+    case 'live':
+      return row('Live updates', 'connected', i.via === null ? '' : `via ${i.via}`, 'ok');
+    case 'reconnecting':
+      return row('Live updates', 'reconnecting', since, 'yellow');
+    case 'offline':
+      return row('Live updates', 'offline', since, 'red');
+    case 'connecting':
+      return row('Live updates', 'connecting', since, null);
+  }
+}
+
+function nodesRow(f: Facts, now: number): StatusRow {
+  if (f.cluster === null) return row('Nodes answering', null, 'not read yet', 'muted');
+  const names = f.cluster.members.map((m) => m.node);
+  const down = names.filter((n) => n in f.unreachable);
+  const text = [
+    `${names.length - down.length} of ${names.length}`,
+    ...down.map((n) => `${n} ${duration((now - f.unreachable[n]) / 1000)}`),
+  ].join(' · ');
+  return row('Nodes answering', null, text, down.length > 0 ? 'yellow' : null);
+}
+
+function historyRow(f: Facts, now: number): StatusRow {
+  const kept = f.cluster?.settings?.history_retention_seconds;
+  const sample = f.lastSample === null ? 'no sample yet' : `last sample ${ago(f.lastSample, now)}`;
+  const text = [kept === undefined ? null : retention(kept), sample].filter((s) => s !== null).join(' · ');
+  return row('Built-in history', null, text, kept === undefined && f.lastSample === null ? 'muted' : null);
+}
+
+function prometheusRow(f: Facts, read: Read): StatusRow {
+  const p = f.prometheus;
+  if (p === null) {
+    return read.at === null
+      ? row('Prometheus', null, 'not read yet', 'muted')
+      : row('Prometheus', null, 'not configured: charts show the built-in history only', 'muted');
+  }
+  const asked = at(p.at);
+  const when = asked === null ? null : `last asked ${clock(asked)}`;
+  switch (p.state) {
+    case 'unasked':
+      return row('Prometheus', null, "configured; no chart has reached past the built-in history yet", 'muted');
+    case 'ok':
+      return row('Prometheus', 'ok', when ?? '', 'ok');
+    case 'failing':
+      return row('Prometheus', 'failing', [p.error ?? null, when].filter((s) => s !== null).join(' · '), 'red');
+  }
+}
+
+function eventsRow(f: Facts, read: Read): StatusRow {
+  if (read.error !== null) return row('Event store', 'failing', read.error, 'red');
+  if (read.at === null) return row('Event store', null, 'not read yet', 'muted');
+  const kept = f.cluster?.settings?.events_retention_seconds;
+  return row('Event store', 'ok', kept === undefined ? '' : retention(kept), 'ok');
+}
+
+/** The console's status rows, in the user menu's order. */
+export function consoleStatus(i: StatusInput, now: number): StatusRow[] {
+  return [
+    liveRow(i, now),
+    nodesRow(i.facts, now),
+    historyRow(i.facts, now),
+    prometheusRow(i.facts, i.read.lag),
+    eventsRow(i.facts, i.read.events),
+  ];
+}
````

`ui/src/model/facts.ts`:

````diff
diff --git a/ui/src/model/facts.ts b/ui/src/model/facts.ts
index bb4e0239488561b59208e11c2ac32679feed7023..5b41742389a8999005fdb81a9950eada834e93dd 100644
--- a/ui/src/model/facts.ts
+++ b/ui/src/model/facts.ts
@@ -9,6 +9,7 @@ import type {
   Job,
   NodeFigures,
   PipelineItem,
+  PrometheusStatus,
   Removal,
   SinkAt,
   SourceAt,
@@ -40,6 +41,10 @@ export interface Facts {
   removals: Removal[];
   /** Each member's own figures, as the stream last sent them. */
   nodes: Record<string, NodeFigures>;
+  /** How the node's Prometheus last answered; `null` when none is configured (or not read). */
+  prometheus: PrometheusStatus | null;
+  /** When the stream's newest chart sample was taken (ms); `null` before the first. */
+  lastSample: number | null;
 }
 
 export function empty(): Facts {
@@ -58,6 +63,8 @@ export function empty(): Facts {
     unheld: {},
     removals: [],
     nodes: {},
+    prometheus: null,
+    lastSample: null,
   };
 }
 
````

`ui/src/model/fixtures.ts`:

````diff
diff --git a/ui/src/model/fixtures.ts b/ui/src/model/fixtures.ts
index e678ee84eb2944467408c78c362ed6d99b5dcfd0..c01129cf7b9d1b9cfa51be4223eb459f142a2cc1 100644
--- a/ui/src/model/fixtures.ts
+++ b/ui/src/model/fixtures.ts
@@ -211,5 +211,7 @@ export function facts(): Facts {
     unheld: {},
     removals: [],
     nodes: Object.fromEntries(['n1', 'n2', 'n3'].map((n) => [n, nodeFigures(n)])),
+    prometheus: null,
+    lastSample: null,
   };
 }
````

`ui/src/model/model.svelte.ts`:

````diff
diff --git a/ui/src/model/model.svelte.ts b/ui/src/model/model.svelte.ts
index 0084eca051160fbe3765d237bdbf96036cf6c913..c6469382b55e65ba3a9c525cdcd5a1f488faaab5 100644
--- a/ui/src/model/model.svelte.ts
+++ b/ui/src/model/model.svelte.ts
@@ -15,6 +15,7 @@ import type {
   Job,
   NodeFigures,
   PipelineItem,
+  PrometheusStatus,
   Removal,
   Series,
   Tables,
@@ -168,8 +169,9 @@ export class Model {
     this.facts.figures[m.id] = m;
   }
 
-  /** A pipeline's chart sample: its lag joins the line, which keeps the last hour. */
+  /** A chart sample: when it was taken; a pipeline's lag joins its line, which keeps the last hour. */
   samples(m: SamplesMsg): void {
+    this.facts.lastSample = Math.max(this.facts.lastSample ?? 0, m.at * 1000);
     const lag = m.series?.lag_seconds;
     if (m.scope !== 'pipeline' || m.pipeline === undefined || lag === undefined) return;
     const points = [...(this.facts.lag[m.pipeline] ?? []), [m.at, lag] as [number, number]];
@@ -273,7 +275,10 @@ export class Model {
           f.removals = (await this.#api.get<{ removals: Removal[] }>('/v1/cluster/removals')).removals;
           break;
         case 'lag': {
-          const { series } = await this.#api.get<{ series: Series[] }>('/v1/history?series=lag_seconds');
+          const { series, prometheus } = await this.#api.get<{ series: Series[]; prometheus?: PrometheusStatus }>(
+            '/v1/history?series=lag_seconds',
+          );
+          f.prometheus = prometheus ?? null;
           f.lag = Object.fromEntries(
             series.filter((s) => s.pipeline !== undefined).map((s) => [s.pipeline!, s.points]),
           );
````

`ui/src/model/types.ts`:

````diff
diff --git a/ui/src/model/types.ts b/ui/src/model/types.ts
index 290d222843f40eeca06c44e81b5af36797361903..99e84387aa5149fa38dd5eb03ae6a318774ead7c 100644
--- a/ui/src/model/types.ts
+++ b/ui/src/model/types.ts
@@ -293,3 +293,11 @@ export interface NodeDetail {
   /** The pipelines whose lease it holds; `null` when the leases couldn't be read. */
   pipelines: { id: string; owner_epoch: number; ring_owner: string | null }[] | null;
 }
+
+/** How the node's Prometheus last answered a chart's read (`GET /v1/history`); absent when no
+ * Prometheus is configured. */
+export interface PrometheusStatus {
+  state: 'unasked' | 'ok' | 'failing';
+  at?: string;
+  error?: string | null;
+}
````

- [ ] **Step 4: Run the tests and the gates**

Run `(cd ui && npm test)`: 205 passed. Then

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
git add -A ui/src/App.svelte ui/src/Frame.svelte ui/src/Frame.test.ts ui/src/lib/status.test.ts ui/src/lib/status.ts ui/src/model/facts.ts ui/src/model/fixtures.ts ui/src/model/model.svelte.ts ui/src/model/model.test.ts ui/src/model/types.ts
git commit -m "feat(ui): the console's status in the user menu — live updates, nodes answering, the history, Prometheus, the event store"
```

---

### Task 7: In a browser: the Cluster flows and two more moves (`ui/e2e`, the E2E suite, the site)

**Files:**
- Modify: `crates/data-avalanche/tests/e2e/console_app.rs`
- Modify: `docs/src/content/docs/deployment/console.md`
- Modify: `ui/e2e/budget.spec.ts`
- Create: `ui/e2e/cluster.spec.ts`

**Interfaces:**
- Consumes: Tasks 3 to 6's pages; `console_app.rs`'s `playwright`, `console`,
  `require_browser!`; `nodes.rs`'s `Cluster::with`, `MORE`; `ui/e2e/common.ts`.
- Produces: `the_cluster_pages_and_a_node_s_actions_in_a_browser` (`cluster.spec.ts`); the
  budget's moves to the Cluster and to a node (D-M16b.8).

- [ ] **Step 1: Write the failing tests**

On three nodes under `basic`: the Cluster page with the console's status; n2's page; n3
asked to leave with a note; n2 shut down once its name is typed. Then n2 has exited 0, both
actions are recorded with who and the note, and n3 still answers, out of the cluster.

`crates/data-avalanche/tests/e2e/console_app.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/console_app.rs b/crates/data-avalanche/tests/e2e/console_app.rs
index 94743739cd021a3f47d05b0ffcfc9759cd165c20..602e9d931c23dc9f7f872fda4c1c76aa1e32847e 100644
--- a/crates/data-avalanche/tests/e2e/console_app.rs
+++ b/crates/data-avalanche/tests/e2e/console_app.rs
@@ -1,6 +1,7 @@
-//! The console's app in a browser (Admin_Console §10; M15, M16a): Playwright's flows (`ui/e2e`)
-//! against a node under each `globals.api.auth` mode, the console switched off, the pipelines'
-//! pages and their actions, and the page budgets (§8) with 50 pipelines on three nodes. Needs
+//! The console's app in a browser (Admin_Console §10; M15, M16a, M16b): Playwright's flows
+//! (`ui/e2e`) against a node under each `globals.api.auth` mode, the console switched off, the
+//! pipelines' pages and their actions, the Cluster pages and a node's actions, and the page
+//! budgets (§8) with 50 pipelines on three nodes. Needs
 //! `DATA_AVALANCHE_BROWSER=1`, the app built into the binary (`npm ci && npm run build` in `ui/`,
 //! then the binary) and Playwright's Chromium (`npx playwright install chromium` in `ui/`);
 //! without the variable each skips itself.
@@ -13,11 +14,14 @@ use data_avalanche_testkit::require_it;
 use crate::console_sessions::session_key;
 use crate::harness::{Doc, E2e, Options, Proc, eventually};
 use crate::idp::{Idp, Provider};
-use crate::nodes::{Cluster, ITEMS};
+use crate::nodes::{Cluster, ITEMS, MORE};
 
 /// The pipelines of the budget's cluster (Admin_Console §8).
 const PIPELINES: usize = 50;
 
+/// `ops:s3cret`, the `basic` user of the flows, as an `Authorization` header.
+const OPS: (&str, &str) = ("Authorization", "Basic b3BzOnMzY3JldA==");
+
 /// Returns early unless the browser's flows are asked for.
 macro_rules! require_browser {
     () => {
@@ -178,6 +182,59 @@ async fn the_pipelines_pages_and_their_actions_in_a_browser() {
     e.cleanup().await;
 }
 
+/// The Cluster pages under `basic` on three nodes (mockups 03, 04, 11): the members and leases, the
+/// console's status, a node's page; n3 asked to leave through its dialog, recorded with its note
+/// and still running out of the cluster; n2 shut down once its name is typed, and gone.
+#[tokio::test]
+async fn the_cluster_pages_and_a_node_s_actions_in_a_browser() {
+    require_it!();
+    require_browser!();
+    let mut c = Cluster::with(
+        "console_app_cluster",
+        ITEMS,
+        MORE,
+        Options {
+            tables: ", snapshots: { mode: never }",
+            cluster: ", rebalance_stabilization: 3s",
+            api: ", auth: basic, user: ops, password: s3cret",
+            globals: &console(""),
+            ..Options::default()
+        },
+    )
+    .await;
+    c.start_all().await;
+    c.settled().await;
+    let env = [("DA_USER", "ops"), ("DA_PASSWORD", "s3cret")];
+    playwright("cluster.spec.ts", c.proc("n1").port(), &env).await;
+
+    let i = c.index("n2");
+    let mut n2 = c.nodes[i].proc.take().expect("n2 ran");
+    assert!(n2.exit().await.success(), "{}", n2.log());
+    let n1 = c.proc("n1");
+    for (kind, node, note) in [
+        ("action.node.leave", "n3", Some("node pool upgrade")),
+        ("action.node.shutdown", "n2", None),
+    ] {
+        eventually(
+            &format!("{kind} is recorded"),
+            Duration::from_secs(30),
+            async || {
+                let path = format!("/v1/events?kind={kind}");
+                let (_, _, body) = n1.exchange_ok("GET", &path, &[OPS], "").await;
+                let v: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
+                v["events"].as_array().is_some_and(|e| {
+                    e.iter()
+                        .any(|e| e["node"] == node && e["note"].as_str() == note)
+                })
+            },
+        )
+        .await;
+    }
+    // n3 left the ring but keeps running, out of the cluster, until it is stopped.
+    assert_eq!(c.proc("n3").answers("/health/live").await, Some(200));
+    c.stop().await;
+}
+
 /// Admin_Console §8: the Overview drawn under 1 s, and moving between pages under 300 ms, with 50
 /// pipelines on three nodes.
 #[tokio::test]
````

`ui/e2e/budget.spec.ts`:

````diff
diff --git a/ui/e2e/budget.spec.ts b/ui/e2e/budget.spec.ts
index 3dfaab85215ab9c0ed42a77c00a06978cd27d2c5..de42f54e1be6deacf444cfad26203260c946364d 100644
--- a/ui/e2e/budget.spec.ts
+++ b/ui/e2e/budget.spec.ts
@@ -72,6 +72,8 @@ test(`moving between pages takes under ${MOVE_MS} ms`, async ({ page }) => {
     ['the pipelines to a pipeline', await move(page, 'tbody a.open[href="/ui/pipelines/p01"]', '.cols tbody td a', 1)],
     ['a pipeline back to the pipelines', await move(page, null, 'tbody a.open', pipelines)],
     ['the pipelines to the Overview', await move(page, 'nav a[href="/ui/"]', '.tile', 5)],
+    ['the Overview to the Cluster', await move(page, 'nav a[href="/ui/cluster"]', '.leases tbody tr', pipelines)],
+    ['the Cluster to a node', await move(page, '.members a[href="/ui/cluster/nodes/n2"]', '.pipelines tbody tr', 1)],
   ] as const;
   for (const [what, ms] of moves) console.log(`${what}: ${Math.round(ms)} ms`);
   for (const [what, ms] of moves) expect(ms, what).toBeLessThan(MOVE_MS);
````

`ui/e2e/cluster.spec.ts`:

````diff
diff --git a/ui/e2e/cluster.spec.ts b/ui/e2e/cluster.spec.ts
new file mode 100644
index 0000000000000000000000000000000000000000..51197c82488d45b50050f35492aa5e3393f3f358
--- /dev/null
+++ b/ui/e2e/cluster.spec.ts
@@ -0,0 +1,72 @@
+import { expect, test, type Page } from '@playwright/test';
+import { env, signIn } from './common';
+
+const user = env('DA_USER');
+const password = env('DA_PASSWORD');
+
+// The flows change the cluster (a leave, then a shutdown): they run in this order.
+test.describe.configure({ mode: 'serial' });
+
+/** Signs in on the way to `path`, and waits for the page to be live. */
+async function signedIn(page: Page, path: string): Promise<void> {
+  await page.goto(path);
+  await signIn(page, user, password);
+  await expect(page.locator('.conn')).toHaveText('live', { timeout: 15_000 });
+}
+
+const dialog = (page: Page) => page.locator('[role="dialog"]');
+const member = (page: Page, n: string) => page.locator('.members tbody tr', { has: page.locator(`a[href="/ui/cluster/nodes/${n}"]`) });
+
+test("the Cluster page: three members alive, their leases, and the console's status from the user menu", async ({ page }) => {
+  await signedIn(page, '/ui/cluster');
+  await expect(page.locator('h2')).toHaveText('Cluster');
+  await expect(page.locator('.members tbody tr')).toHaveCount(3);
+  for (const n of ['n1', 'n2', 'n3']) await expect(member(page, n)).toContainText('alive');
+  await expect(page.locator('.leases tbody tr')).toHaveCount(3);
+  await page.click('.who summary');
+  const status = (label: string) =>
+    page.locator('.who .status tr', { has: page.getByRole('cell', { name: label, exact: true }) });
+  await expect(status('Live updates')).toContainText('connected via n1');
+  await expect(status('Nodes answering')).toContainText('3 of 3');
+  await expect(status('Built-in history')).toContainText('24 h');
+  await expect(status('Prometheus')).toContainText('not configured');
+  await expect(status('Event store')).toContainText('ok 30 days');
+});
+
+test("a node's page: its pipelines, its charts drawn, what its gossip sees", async ({ page }) => {
+  await signedIn(page, '/ui/cluster');
+  await page.click('.members a[href="/ui/cluster/nodes/n2"]');
+  await expect(page).toHaveURL(/\/ui\/cluster\/nodes\/n2$/);
+  await expect(page.locator('h2 .chip')).toHaveText('alive');
+  await expect(page.locator('.charts canvas')).toHaveCount(3);
+  await expect(page.locator('.gossip')).toContainText(/n1 alive/, { timeout: 15_000 });
+});
+
+test('n3 leaves through its dialog with a note, and the others carry on without it', async ({ page }) => {
+  await signedIn(page, '/ui/cluster/nodes/n3');
+  await page.click('.actions >> text=Leave…');
+  await expect(dialog(page)).toContainText('n3 leaves the cluster?');
+  await expect(dialog(page)).toContainText(`Recorded as: n3 left the cluster, asked by ${user} (admin)`);
+  await page.fill('[role="dialog"] textarea', 'node pool upgrade');
+  await page.click('[role="dialog"] button.go');
+  await expect(dialog(page)).toHaveCount(0);
+  await page.click('nav a[href="/ui/cluster"]');
+  await expect(member(page, 'n3')).toHaveCount(0, { timeout: 60_000 });
+  await expect(page.locator('.leases tbody tr')).toHaveCount(3);
+});
+
+test('n2 shuts down only once its name is typed', async ({ page }) => {
+  await signedIn(page, '/ui/cluster/nodes/n2');
+  await page.click('.actions >> text=Shut down…');
+  const go = page.locator('[role="dialog"] button.go');
+  await expect(dialog(page)).toContainText('Type n2 to confirm.');
+  await expect(go).toBeDisabled();
+  await page.fill('[role="dialog"] input.typed', 'n');
+  await expect(go).toBeDisabled();
+  await page.fill('[role="dialog"] input.typed', 'n2');
+  await page.click('[role="dialog"] button.go');
+  await expect(dialog(page)).toHaveCount(0);
+  await page.click('nav a[href="/ui/cluster"]');
+  await expect(member(page, 'n2')).toHaveCount(0, { timeout: 60_000 });
+  await expect(page.locator('.members tbody tr')).toHaveCount(1);
+});
````

- [ ] **Step 2: Run them: they pass, on the pages Tasks 3 to 6 built**

Run (with the emulators up): `(cd ui && npm run build) && DATA_AVALANCHE_IT=1 DATA_AVALANCHE_BROWSER=1 DATA_AVALANCHE_UI=required cargo test -p data-avalanche --features e2e-hooks --test e2e console_app -- --test-threads=1`
Expected: PASS, seven tests: the flows check Tasks 3 to 6's pages, which are built (the Cluster
flows 6 s; the budget's moves: the Overview to the Cluster 6 ms, the Cluster to a node 9 ms).

- [ ] **Step 3: Implement**

The site's console page: the Cluster, a node's page, acting on nodes, the console's
status, the removal rule, and the notices as the page prints them.

`docs/src/content/docs/deployment/console.md`:

````diff
diff --git a/docs/src/content/docs/deployment/console.md b/docs/src/content/docs/deployment/console.md
index f4583f0344545162071e0b795430de1e4c2fca11..95cf4dde8255327c2b493ef86c2803ccc55970a6 100644
--- a/docs/src/content/docs/deployment/console.md
+++ b/docs/src/content/docs/deployment/console.md
@@ -30,7 +30,8 @@ cookies are `Secure`, which browsers send over plain HTTP only to `localhost`.
 
 Every page has the same top bar: the sections, the live connection's state, the cluster's config
 generation, and who is signed in, with their role and the sign-out. This build has the
-**Overview** and the **Pipelines**; the other sections say they aren't in this build yet.
+**Overview**, the **Cluster** and the **Pipelines**; the other sections say they aren't in this
+build yet.
 
 The page shows the cluster as it is when it opens, then follows the cluster's live updates
 (`GET /v1/stream`) through whichever node served it, so it changes as things happen without a
@@ -55,11 +56,66 @@ page; a later release makes them configurable):
 |---|---|
 | Red | A table halted; a pipeline stopped by an error; a pipeline no node has held for over 30 s; a node the others' probes get no answer from |
 | Orange | A pipeline's lag over 60 s and higher than 5 min ago; a slot holding WAL past 50% of `max_slot_wal_keep_size` (16 GB when the server sets none); a lease lost or taken over in the last hour; a snapshot job failed in the last 24 h; a node the console can't follow; a pipeline that can't commit |
-| Yellow | A node on another config generation; a restart-only change waiting on a node; a pipeline paused or stopped by a person over 1 h ago |
+| Yellow | A node on another config generation; a restart-only change waiting on a node; a pipeline paused or stopped by a person over 1 h ago; a removed pipeline whose teardown hasn't finished after 2 min |
 
 A node the console can't follow shows its pipelines as last known, marked with how long it has been
 out of reach.
 
+### The cluster
+
+**Cluster** shows the members and the leases:
+
+- tiles: the members by state, the leases held, the config generation (and the nodes on another),
+  the pipelines away from their ring owner, and the cluster's settings (tokens, lease duration,
+  lock backend);
+- every member: its state (`alive`, `suspect` when the others' probes miss it, which keeps it in
+  the ring, `leaving` while it hands off, `joining`), its address, version, config generation, the
+  pipelines it holds, its slowest lease renewal in the last hour, and how long it has been up;
+- the pipelines **away from their owner** after a failover, each with why (the lease lost or taken
+  over in the last hour, from the events) and when it goes back: the ring owner takes it back once
+  the membership has been stable for `rebalance_stabilization`, `max_concurrent_handoffs` at a
+  time. One away for over an hour, or with no hand-off recorded, is red;
+- every lease: its holder, ring owner, owner epoch, when it expires, and its pipeline's state,
+  those with no holder or not renewed first;
+- the **pending removals**, while any: a pipeline removed from the config whose teardown (its
+  slot, its state) hasn't finished, the generation that removed it, and since when
+  (`GET /v1/cluster/removals`).
+
+A node's page (`/ui/cluster/nodes/<name>`, from its row) has its state, its API and gossip
+addresses, version, config generation and when it started; the pipelines it holds, with their lag;
+its lease renewals, memory (against its limit) and CPU over 6 hours; what it recorded; and what its
+gossip sees: its peers and their states, its last probe, and the suspicions of the last day. Each part is read from the node itself, through whichever
+node served the page.
+
+### Acting on nodes
+
+An `admin` sees two actions on a node's page:
+
+- **Leave**: the node hands its pipelines to the others, one at a time, and leaves the ring. The
+  process keeps running, out of the cluster, until it is stopped or restarted. Each hand-off pauses
+  its pipeline for a while; a running snapshot goes on from its last chunk where its pipeline goes.
+- **Shut down**: a leave, then the process exits. A supervisor such as Kubernetes starts it
+  again and it rejoins; without one, nothing restarts it. The dialog asks for the node's name typed
+  before it acts.
+
+There is no in-place restart. Each dialog names the pipelines the node holds, takes a note, and
+shows the line it will be recorded as (`n2 left the cluster, asked by Oleksii (admin)`); the node
+acted on checks the role again and records it. When the console's live updates come through that
+node, the dialog says so: the page reconnects to another node by itself. An `operator` or a
+`viewer` sees no node actions.
+
+### The console's status
+
+The user menu (a click on your name) shows the console's own state:
+
+| | |
+|---|---|
+| Live updates | `connected` and through which node, or `reconnecting` / `offline` with the last update's age |
+| Nodes answering | how many members the console follows, and for how long each other one has been out of reach |
+| Built-in history | its retention (`globals.console.history`) and when the last sample came |
+| Prometheus | `not configured`, `ok`, or `failing` with its error, and when the node last asked it; read again as the menu opens |
+| Event store | `ok` with its retention (`globals.console.events`), or `failing` with why |
+
 ### The pipelines
 
 **Pipelines** lists every pipeline, the worst first: its state, source and sink, the node that runs
@@ -79,14 +135,16 @@ behind the stream reads), its snapshot jobs, and what happened to it. Hovering a
 chart highlights it in the list below, and the reverse.
 
 Every part of the page says how old its data is. A part that couldn't be read keeps what it had and
-says so with its age (`Couldn't be read; showing what was last read: its snapshot jobs (HTTP 503),
-last read 20 min ago`, or `…, never read`), on the list as on a pipeline's page, the source's panel
+says so with its age and the server's reason (`Couldn't be read; showing what was last read: the
+snapshot jobs (the request timed out), last read 20 min ago`, or `…, never read`), on every page,
+the source's panel
 says when it was read (`read 40 s ago`), and while the live connection is stale every panel is
 dimmed and the banner says for how long (`Not current: as of 14:02:00 · 34 s old`, or `Not current:
 no live update yet` before the first message). The lease line counts down to the lease's end
 (`expires in 8 s`) and says `expired 3 s ago` when the end has passed without a renewal, and `lease
 released` once its holder let go; a pipeline's page reads the cluster again every 5 s while it is
-shown.
+shown. While the cluster can't be read, the lease line counts nothing down (its end is only what
+the last read saw), and the page's notice names the cluster.
 
 ### Acting on pipelines
 
````

- [ ] **Step 4: Run the tests and the gates**

Run the E2E command of Step 2. Then

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
git add -A crates/data-avalanche/tests/e2e/console_app.rs docs/src/content/docs/deployment/console.md ui/e2e/budget.spec.ts ui/e2e/cluster.spec.ts
git commit -m "test(e2e): the Cluster pages and a node's actions in a browser on three nodes, two more moves within 300 ms; the site"
```

---

## Exit

- Spec patch S-M16b.1 applied (2026-11-26, before Task 1).
- Every task committed, one commit each, after the plan's commits; the gates pass on the last one
  (`cargo fmt --all --check`, both clippy runs, `cargo test --workspace`, and in `ui/`
  `npm run check`, `npm test`, `npm run build`).
- With the emulators up, the app built in and Chromium installed: the whole E2E suite on
  PostgreSQL 18 with `DATA_AVALANCHE_UI=required DATA_AVALANCHE_BROWSER=1`, the console's seven
  flows among it, the navigation budget's six moves each under 300 ms.
- `cargo xtask docs --check` and `cd docs && npm run build` pass.
- DA-API-21 ✅ in the gap tracker, and `Admin_Console.md` §4.4 checked against what was built.
- At the plan's verification (the code above, on `7591ffe`, each task's red and green commits
  apart, the diffs replayed to the same trees): every Step 2 failure as stated, and each green
  as stated (Task 1: 240 of the API crate's lib tests; the UI: 154, 171, 186, 197, 205); at
  Task 7, `cargo test --workspace` passed, both clippy runs and `cargo fmt --all --check` clean,
  svelte-check 0 errors and 0 warnings (and none with `noUnusedLocals` on), `cargo xtask docs
  --check` held and the site built; the console's seven browser flows passed together (100 s):
  the first page drawn in 272 ms, the moves 17, 11, 13, 9, 6 and 9 ms.
  Two faults the browser found, which the unit tests didn't, are fixed in the tasks above:
  - the Cluster page hung its tab: the app's effect called `Renewals.load`, which reads and
    writes `max` and `read` before its first `await`, so each answer ran the effect again. It
    now runs under `untrack`, and `App.test.ts` checks each member's renewals are read once
    (Task 3);
  - the menu's status table, drawn while closed (a closed `<details>` keeps its content in the
    page), added its five rows to every page's `tbody tr`. It is drawn only while open (Task 6).
- Then, on Oleksii's request: the whole-branch review and its fixes, the push, the PR.

## Deviations during execution

- **Task 3, `ui/src/cluster/{Cluster.svelte,Cluster.test.ts}` (fix commit after the task's own).**
  The task review found the leases' rows untoned: only their State cell was coloured, where
  Admin_Console §4 and mockup 03 ask for "orange lease rows" (the members' and the away panel's
  rows were toned already). Each lease row now takes its state's tone (`class={l.state.tone}`:
  orange away from its owner, red with no holder or not renewing), on the page's existing
  `tr.<tone>` rules; the page test checks the away row is orange and a held one isn't. No later
  task touches these files.
- **Task 6, `ui/src/{Frame.svelte,Frame.test.ts}` (fix commit after the task's own).** The task
  review asked whether `bind:open` updates before the menu's toggle handler runs; it doesn't:
  Svelte attaches the handler first (`$.event`, then `$.bind_property`, both `addEventListener`
  at once), so the plan's handler (`() => open && onStatus()`) read the old value, and the menu
  read the status again as it closed, not as it opened (D-M16b.6). The handler now reads the
  element (`(e) => e.currentTarget.open && onStatus()`), and the test checks the read right after
  opening, which the plan's test, counting one call over an open and a close, couldn't tell
  apart. No later task touches these files.
- **Task 7.** The console's seven browser flows on three nodes passed (102 s): the first page
  drawn in 207 ms, the moves 18, 13, 41, 11, 9 and 13 ms.
- **The whole-branch review** (Opus) found no Critical issue and four Important ones, fixed in
  one wave (`75c8a1f`) and re-reviewed:
  - the Cluster page counted every lease down from one read, so within a lease term each healthy
    lease read "expired": a renewal sends no event. The Cluster page and a node's page now read
    the cluster again every 5 s (`CLUSTER_REREAD_MS`), as M16a's pipeline page does;
  - a node's page fell back to the detail's phase once the status no longer listed the member,
    so a node that crashed or left showed `alive`: it now says `not a member` (grey);
  - the dialogs said the page "reconnects to another node on its own" when the console's stream
    comes through the node acted on. A leave keeps the process and its streams up, out of the
    cluster; a shutdown's reconnect goes to the address the console was opened at. The lines now
    say so (this corrects D-M16b.5's wording; the site and `Admin_Console.md` §4.4 follow).
    Whether a leave should close its live streams, so that browsers reconnect through the load
    balancer, was put to Oleksii: no (2026-11-26), a leave keeps them, and the lines stand;
  - a removal ended without an event (undone by a later generation, or ended while a peer was out
    of reach) stayed listed and turned yellow after 2 min: the removals are read again when a
    peer is followed again, and every 30 s while any is listed (`REMOVALS_REREAD_MS`).

  And from the task reviews' minors: the Leases tile counts leases not renewed; the Members tile
  names a joining member and isn't green then; the shutdown dialog names the members that can
  carry the pipelines (active, not suspect) when not every other one can; the budget's move to a
  node waits for a pipeline's link, not any row; the site names the lease column's state and its
  row colours. D-M16b.1's reason for a route of its own ("the status, memory only") was wrong:
  the status reads each lease from storage; the route stands, for the reason
  `Admin_Console.md` §4.4 now gives. After the wave: 1 854 workspace tests, 212 UI tests, the
  console's seven browser flows (the first page in 250 ms, the moves 19, 12, 16, 14, 11 and
  28 ms).
- **The whole E2E suite** at the wave (`c8a1955`), on PostgreSQL 18 with the browser flows: 66
  passed (938 s); the first page drawn in 334 ms, the moves 15, 11, 40, 10, 9 and 16 ms.

