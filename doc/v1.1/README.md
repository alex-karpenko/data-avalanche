---
updated: 2026-11-15
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
| [`M13a-console-events.md`](M13a-console-events.md) | `globals.console` and the console's oauth2 fields (V-128…V-136); the cluster's event timeline in `globals.state`; who did each mutating call, refused attempts too; the cluster's own events; `GET /v1/events`; the events page and alert | **Executed 2026-11-15**: seven tasks, the whole-branch review and its fixes; V-137 added (spec `c4184d7`); deviations in the plan. PR open, not merged |
| M13b | The owner-served pipeline detail and per-table figures, source detail, all tables, all snapshot jobs, job cancel and retry ("copy cancelled"), node leave and shutdown through any node, config generations and diff with each node's file status, process and gossip figures, the gossip's API address and file hash (MAC label v2) | To be planned after M13a |
| M13c | The history sampler and store, the optional Prometheus reader, the SSE stream | To be planned |
| M14 | Sign-in and sessions (basic, oauth2, jwt), the 401 and 403 pages | To be planned |
| M15 | The console's frame and Overview, served under `/ui/`; the chart's console values and NOTES | To be planned |
| M16 | The detail pages and their actions | To be planned |
