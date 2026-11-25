---
updated: 2026-11-25
domain: Architecture
tags: [v1.1, implementation-plan, index, console]
---

# v1.1 planning artifacts

This folder holds the plans for **building** 1.1.0, the embedded admin console. Its design is
`doc/research/Admin_Console.md` and its spec changes are in `doc/specs/` (the web console,
`globals.console`, V-128…V-136). The approved page mockups are in [`mockups/`](mockups/). When a
plan and a spec disagree, the spec wins: stop and raise it.

The plans follow `doc/v1.0/README.md`: how they are written and run, the test gates, the release
checklist and the commit conventions all apply unchanged. 1.1.0 is one release built as several
milestones, each on its own branch with its own PR, and nothing is tagged until the last one
merges (Admin_Console §14).

## Index

| File | What it is | Status |
|---|---|---|
| [`mockups/`](mockups/) | The console's pages: Overview, Cluster, node, Pipelines, pipeline, table, snapshot jobs, Events, Config, sign-in and errors, node actions and degraded states | Approved 2026-11-14 |
| [`M13a-console-events.md`](M13a-console-events.md) | `globals.console` and the console's oauth2 fields (V-128…V-136); the cluster's event timeline in `globals.state`; who did each mutating call, refused attempts too; the cluster's own events; `GET /v1/events`; the events page and alert | **Executed 2026-11-15**: seven tasks, the whole-branch review and its fixes; V-137 added (spec `c4184d7`); deviations in the plan. **Merged (#27)** |
| [`M13b-console-nodes.md`](M13b-console-nodes.md) | The gossip's v2 (each node's API address and file hash; `globals.api.advertise_url`, V-138); a node's detail and its actions (leave, shutdown, config) through any node with the caller's own credentials; process and gossip figures; config generations with who published each and why, their texts with secrets masked, the diff, each node's file | **Approved 2026-11-15**, with spec patch S-M13b.1 (applied); **Done**: nine tasks, whole-branch review and its fixes, spec patches S-M13b.1 and S-M13b.2. **Merged (#28)** |
| [`M13c-console-jobs.md`](M13c-console-jobs.md) | Every pipeline's snapshot jobs; a job's figures (rows against the source's estimate, rate, ETA, what lake readers see); cancel a job (`COPY_CANCELLED`) and retry it; M13b's and M13a's carry-ins (scheme-relative URLs masked, the events' expiry batched) | **Approved 2026-11-17**, with spec patch S-M13c.1 (applied); **Done**: nine tasks, whole-branch review and its fixes, spec patches S-M13c.1 and S-M13c.2; deviations in the plan. **Merged (#29)** |
| [`M13d-console-pipelines.md`](M13d-console-pipelines.md) | A pipeline's live figures from the node that runs it, asked of any node; the source's detail (PostgreSQL's slot, WAL held, publication; MySQL's position and binlog expiry); each table's figures and freshness; a table's page (halt and its time, Delta facts, columns against the source's, jobs); every pipeline's tables | **Approved 2026-11-18**, with spec patch S-M13d.1 (applied); amended 2026-11-18 with S-M13d.2 (the `copied` state, a Task 5 fix round); **Done**: nine tasks, whole-branch review and its fixes, spec patches S-M13d.1, S-M13d.2 and S-M13d.3; deviations in the plan. **Merged (#30)** |
| [`M13e-console-history.md`](M13e-console-history.md) | The chart history: each node samples the series it owns (a pipeline's lag, changes, WAL held, binlog lag and snapshot rate; its tables' changes, MERGE cost and commit time; the node's memory, CPU and lease renewals) into `globals.state`, kept for `globals.console.history.retention`; `GET /v1/history` from any node; a pipeline's older range from `globals.console.prometheus` | **Approved 2026-11-19**, with spec patch S-M13e.1 (applied); amended 2026-11-19 with S-M13e.2 (an asked step keeps at most 1440 points, a Task 4 fix round); **Done**: seven tasks, whole-branch review and its fixes, spec patches S-M13e.1 and S-M13e.2; deviations in the plan. **Merged (#31)** |
| [`M13f-console-stream.md`](M13f-console-stream.md) | The live stream: `GET /v1/stream`, server-sent events from any node; each node publishes its pipelines' states as they change, their live figures and its own every 5 s, each chart sample and each event written; the node asked follows every other member's stream with the caller's credentials and merges them; no replay | **Approved 2026-11-19**, with spec patch S-M13f.1 (applied); **Done**: six tasks, whole-branch review and its fixes; deviations in the plan. **Merged (#32)** |
| [`M14-console-sessions.md`](M14-console-sessions.md) | Sign-in under each `globals.api.auth` mode: `basic`'s form (failures counted per address), `oauth2`'s authorization code with PKCE, `jwt`'s token, `none`'s banner; a session in an encrypted cookie every node opens and verifies again, lasting `session_ttl` from sign-in; the API takes it, with an anti-forgery token for what changes something; peers verify it; a stream ends with its credentials; the 401 and 403 pages, rendered by the server | **Approved 2026-11-24**, with spec patch S-M14.1 (applied); **Done**: six tasks, whole-branch review and its fixes, spec changes for the `basic` attempt limit and V-129 (`public_url` an origin, no user information); deviations in the plan. **Merged (#33)** |
| [`M15-console-overview.md`](M15-console-overview.md) | The console's app (Svelte, built into the binary by the API crate's `build.rs`, served under `/ui/`): the frame (the session kept fresh, the `/v1` client with the anti-forgery token, the live stream and its state, the sections) and the Overview (tiles, needs attention with fixed thresholds stated on the page, the pipelines worst first with the last hour's lag, the newest events); the flows in a browser (Playwright: sign-in per mode, the Overview live, the console off, the first page within 1 s with 50 pipelines); `/v1/pipelines` reads its pipelines at once (bounded); the image and CI build the app; the chart's console values and NOTES | **Approved 2026-11-25**; **Done**: six tasks, two task fix rounds, the whole-branch review and its fixes; deviations in the plan. **Merged (#34)** |
| [`M16a-console-pipelines.md`](M16a-console-pipelines.md) | The pieces every page shares (the cluster's model, the chart on uPlot with its events and hover links, the action dialog, lists with their view in the address and keys) and the Pipelines section: the list (worst first, filters and sort, a selection acted on one at a time), a pipeline's page (header and lease, actions, the lag over 6 hours with its events, tables, source, snapshot, activity); `/v1/pipelines` names source and sink; the flows in a browser and moving between pages within 300 ms | **Written 2026-11-25**, awaiting approval |
| M16b | The Cluster and a node's page, node actions, pending removals (DA-API-21) and the rule, the console's status | To be planned |
| M16c | Tables and a table's page, Snapshots and a job's page, job actions | To be planned |
| M16d | Events (histogram, export) and Config (the running config, generations and their diff, nodes, validate and reload); the Overview's actions read past the events' scan limit | To be planned |

## For the 1.1.0 release notes

Collected as the milestones land, for the release's notes, which the upgrade runbook's "Rolling
back" (`docs/src/content/docs/runbooks/upgrade.md`) points at.

- **Rolling back to 1.0 after a snapshot job was cancelled** (M13c): a `COPY_CANCELLED` halt is a
  halt code 1.0 doesn't know, so a 1.0 node refuses that pipeline's halts and the pipeline's start
  fails. Before rolling back, copy each such table again (retry its job), which clears the halt. A
  1.0 node also skips every job a 1.1 node asked (each records who asked, by role).
- **Rolling back to 1.0 with a halted table** (M13d): 1.1 records when each halt began (`since` in
  the pipeline's `halts.json`), a field a 1.0 node refuses, so the pipeline's start fails on 1.0.
  Before rolling back, remove `since` from each such pipeline's `halts.json`; the halts stay.
- **Rolling back to 1.0 leaves the chart history** (M13e): 1.1 keeps it under `history/` in
  `globals.state`, which a 1.0 node neither reads nor expires. Delete that folder after rolling
  back, or leave it: nothing else reads it.
