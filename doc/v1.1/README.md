---
updated: 2026-11-19
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
| [`M13e-console-history.md`](M13e-console-history.md) | The chart history: each node samples the series it owns (a pipeline's lag, changes, WAL held, binlog lag and snapshot rate; its tables' changes, MERGE cost and commit time; the node's memory, CPU and lease renewals) into `globals.state`, kept for `globals.console.history.retention`; `GET /v1/history` from any node; a pipeline's older range from `globals.console.prometheus` | **Approved 2026-11-19**, with spec patch S-M13e.1 (applied) |
| M13f | The live stream (SSE): each node's live changes, the serving node merging its peers' into the one stream a page reads | To be planned |
| M14 | Sign-in and sessions (basic, oauth2, jwt), the 401 and 403 pages | To be planned |
| M15 | The console's frame and Overview, served under `/ui/`; the chart's console values and NOTES | To be planned |
| M16 | The detail pages and their actions | To be planned |

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
