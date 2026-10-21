---
updated: 2026-10-21
domain: Clustering
tags: [v1, implementation-plan, m8a, cluster, gossip, swim, foca, ring, placement, reconciler, hand-off, discovery, e2e, fault-injection]
resolves: [DA-CLUS-14, DA-OPS-7, DA-DEP-4]
sources: [SWIM (Cornell), Consistent Hashing]
---

# M8a — Cluster Membership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run several nodes as one cluster. Nodes find each other by gossip, and each pipeline runs
on the node the ring gives it, never on two at once. The cluster has to survive:
- a node that dies: its pipelines are taken over after the lease;
- a node that leaves: its pipelines are handed off with no lease wait;
- a partition: nothing moves while the leases hold;
- a stop-the-world pause: the zombie is fenced and rejoins;
- a rolling restart: hand-offs are capped by `max_concurrent_handoffs`.

M6b built the lease, the controller and a self-only ring. M8a replaces the self-only ring with one
derived from gossip membership.

M8 is split (Q-M8-3). **M8a** (this plan) covers the gossip, the ring, the flap rules, the caps, the
leave, discovery, the two items M6b carried (DA-CLUS-14, DA-OPS-7) and the multi-node E2E suite.
**M8b** covers config generations at run time (publish, gossip nudge plus poll, adoption) and the
reload E2E.

**Architecture:** Every new piece lives in `data-avalanche-cluster`; the binary wires them up.

- **The config** (Task 1): `globals.cluster.memberlist` describes foca's gossip. The keys only the
  `memberlist` crate used are removed: TCP timeouts, compression, Lifeguard, TLS and
  `dead_node_reclaim_time`. V-122 bounds the gossip timers.
- **The ring** (Task 2): `tokens` xxh3 points per node; a pipeline's owner is the first point at or
  after its id's hash.
- **The wire** (Task 3): each UDP packet carries a keyed BLAKE3 MAC whose key is derived from the
  anchor's `fingerprint_key` (Q-M8-2). The tests run on an in-memory hub that can cut and heal
  links.
- **The gossip** (Task 4): foca 2.0 inside a tokio actor of ours (Q-M8-1). A node's identity
  carries its name, incarnation, revision, address, phase, config generation and build. The
  members are each name's latest live identity, suspects included.
- **Placement** (Task 5): the ring over the Active members. A removal applies at once; an addition
  applies after `rebalance_stabilization` of continuous eligibility (D-11).
- **The reconciler** (Task 6) recomputes the targets on every members change and every resync. It
  also:
  - caps hand-offs with `max_concurrent_handoffs`;
  - restarts a controller that ended unasked (DA-OPS-7);
  - leaves its leases to expire when dropped, as a crash would.
- **The leave** (Task 7): Draining takes the node off every ring at once. Its pipelines are handed
  off and released, then the gossip's leave tells the others.
- **A lake ahead of the lease** (Task 8, DA-CLUS-14): a worker fenced by a table's higher epoch
  takes over its own lease above that epoch, once. A second fence after that jump is an `ERROR`
  naming another lease store.
- **Discovery and the address** (Task 9): `static` or `dns` seeds, asked again while the node has
  no peer. The advertised address comes from config, else a specific bind address, else a private
  IPv4. A seeded node waits up to 5 s for its first peer before it turns Active.
- **Three in-process nodes** (Task 10): spread, a crash, a partition and a rolling restart, on the
  hub. No pipeline ever runs on two nodes.
- **The binary** (Task 11) puts it together:
  - it binds UDP, derives the key and starts the gossip, discovery and the join;
  - it runs the reconciler on the members;
  - on SIGTERM it leaves the cluster.

  E2E nodes take their name and ports from the environment (D-3).
- **The E2E suite** (Task 12, Q-M8-4): three node processes on loopback, each behind a UDP
  forwarder. The scenarios: `kill -9`, a rolling SIGTERM, a partition the forwarders make, and a
  SIGSTOP past the lease.

**Tech Stack:** Rust 1.98.1 (edition 2024). New external crates (roadmap §2.1):
- `foca` `=2.0.0` (`default-features = false`, features `std`, `serde`, `postcard-codec`);
- `rand` `0.10` (`std`, `std_rng`, `sys_rng`), foca's random source;
- `xxhash-rust` `0.8` (`xxh3`), the ring's hash;
- `if-addrs` `0.15`, the host's addresses.

`blake3`, `bytes`, `serde` and `thiserror` come from the workspace.

**Spec:**
- `doc/v1.0/00-roadmap.md`: §2.1 (the membership pin, R-1), §3.7 (the cluster), §4 M8 (scope and
  exit), §5 (tests), §6 R-1.
- `doc/v1.0/M6b-single-node-control.md`: D-M6b.4…8 (the lease, the controller, the self-only
  ring), and the carried items of its exit (DA-CLUS-14, DA-OPS-7).
- `prototypes/cluster-capability/README.md`: the probe gate (memberlist C1–C13, foca F1–F8) and
  its findings 4–7.
- `doc/research/Clustering_Architecture_Research.md`:
  - §2–§3: membership, NodeMeta, phases, incarnation, join and leave;
  - §4: placement on the ring;
  - §5: the lease;
  - §7: partitions and pauses;
  - §8: reassignment, drain, rate limits;
  - §10: single node and discovery;
  - §20: D-9, D-10, D-11.
- `doc/specs/Configuration_Specification.md`: §6.2 (the anchor), §6.5 (`globals.cluster`),
  §6.5.1 (`memberlist`), §13 (V-054, V-068, V-094…V-096), §14 (the example).
- `doc/specs/System-Overview.md`: the cluster-state row, OQ-API1 (TLS).

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`.
- **Dependency direction** (roadmap §2): `cluster → pipeline, state, storage, config, core,
  telemetry`; the binary depends on every crate. `testkit` is a dev-dependency only. The cluster
  crate opens no TCP socket: the gossip is UDP alone.
- **The gossip's packets:** the payload, then a 32-byte keyed BLAKE3 MAC. The key is
  `blake3::derive_key("data-avalanche 2026-10-20 gossip packet MAC v1", fingerprint_key)`, with
  the anchor's 32-byte `fingerprint_key`. Packets aren't encrypted. A payload is at most 1400
  bytes.
- **Placement:** a point on the ring is `xxh3_64(node name ‖ index as big-endian u32)`, for
  `index` in `0..tokens`. A pipeline's key is `xxh3_64(pipeline id)`. Each node computes the ring
  from the members it sees; nothing about the ring is gossiped.
- **Time in tests:** unit and in-process tests run on tokio's paused clock
  (`start_paused = true`), with no wall-clock waits. The E2E suite runs real processes.
- **Test gates:**
  - Plain `cargo test` stays hermetic: the gossip's tests run on the in-memory hub, with one UDP
    test on loopback.
  - The E2E suite needs `DATA_AVALANCHE_IT=1` and the `e2e-hooks` feature, and runs with
    `--test-threads=1`. It runs on either source (`DATA_AVALANCHE_E2E_SOURCE`).
- **Credentials:** the tests use the compose services' fixed test credentials only, and nothing
  else goes in code, docs or output. Tests generate their keys at run time. Never print the files
  under `test-infra/.tls/`.
- **Public-API conventions:** public types are re-exported at the crate root. Errors are
  `thiserror` types (`ClusterError`, new in Task 9).
- Code blocks are the verified content, already `rustfmt`-formatted. Diffs are `git apply`-ready;
  a step's diffs apply in the order given, after the earlier steps'.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to break a cluster in production. Each is pinned by a test in the
task after the arrow.

1. **A pod restarted under the same name at a new IP** (a StatefulSet pod rescheduled). The
   restarted node must count as the member at once, and its old identity must not linger as a
   second node or a suspect in the ring. Otherwise its pipelines wait for a suspicion timeout, or
   two identities race for them → Task 4 (`a_restart_at_a_new_address_counts_at_once`,
   `the_members_are_each_names_latest_live_identity`), Task 10
   (`a_rolling_restart_keeps_every_pipeline_running_once`).
2. **A gossip partition while storage stays reachable.** Each side sees the other gone and computes
   its own ring. No pipeline may move while its holder renews: the leases decide, not the ring.
   The healed cluster must rejoin without a restart → Task 4
   (`a_partition_splits_the_members_and_a_heal_joins_them`), Task 10
   (`a_partition_moves_no_pipeline_while_the_leases_hold`), Task 12
   (`a_partition_moves_no_pipeline`).
3. **A stop-the-world pause past the lease** (SIGSTOP, a VM stall). The others take its pipelines
   over. When the paused node resumes, its stale worker is fenced: nothing is committed twice, and
   it rejoins as a member → Task 12 (`a_paused_holder_loses_its_pipelines_and_rejoins`). A table
   whose epoch ran ahead of the lease (a restored `globals.state`) gets one jump, not an endless
   climb → Task 8 (`a_worker_fenced_goes_back_to_the_lease_above_the_tables_epoch`,
   `a_second_fence_past_a_jumped_epoch_is_an_error`).
4. **A node of another cluster on the gossip port**, or stray traffic. A node on another anchor
   (another `globals.state`) shares no key, so it can't join or disturb the cluster. Its packets
   are dropped, with at most one warning a minute → Task 3
   (`another_anchor_is_another_key`, `a_sealed_packet_opens_and_a_changed_one_doesnt`), Task 4
   (`a_node_with_another_key_cant_join`).
5. **A joiner that flaps, and a rolling restart of every node.** A flapping joiner must not pull
   pipelines back and forth. Each restart must hand its pipelines off with no lease wait, at most
   `max_concurrent_handoffs` at a time → Task 5
   (`a_joiner_waits_out_the_stabilization_and_a_flap_restarts_its_wait`), Task 6
   (`the_hand_off_cap_drains_one_pipeline_at_a_time`), Task 7
   (`a_leave_hands_every_pipeline_off_without_a_lease_wait`), Task 12
   (`a_rolling_restart_hands_off_and_loses_nothing`).

## Decisions and rulings

**Asked and answered** (2026-10-20, all four "Recommended"):

| # | Question | Answer |
|---|---|---|
| Q-M8-1 | Which gossip crate, after the probe gate? | **`foca` 2.0, with our own tokio actor**. `memberlist` 0.8.5 failed the gate: reaping corrupts its member table (C13, a panic), and member states are hidden. foca passed F1–F8 (`prototypes/cluster-capability`) |
| Q-M8-2 | How is the gossip secured? | **A keyed BLAKE3 MAC on every packet**, keyed from the anchor's `fingerprint_key`. No encryption. `memberlist.tls` is dropped (V-054 retired) |
| Q-M8-3 | One plan or two? | **M8a + M8b**. M8a: gossip, ring, flap rules, caps, leave, discovery, the carried items, the multi-node E2E. M8b: config generations at run time and the reload E2E |
| Q-M8-4 | How does the E2E run several nodes? | **Three host processes**, UDP forwarders for the partition, SIGSTOP/SIGCONT for the pause, `kill -9` and SIGTERM. The docker-compose 3-node run moves to M11 (the Helm and kind smoke) |

**Rulings where the plan decides** (each pinned by a test):

| # | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M8a.1 | **The identity.** foca's identity is `NodeIdentity { node_id, incarnation, rev, addr, meta }`, with `meta = NodeMeta { phase, config_generation, build }`. `incarnation` is the process's start in Unix ms. foca keys it by `addr`. `renew()` bumps `rev`, and `win_addr_conflict` keeps the higher `(incarnation, rev)` | foca accepts an announce only at the receiver's own address, and knows a seed by its address alone (probe finding 4). The name is what the ring hashes, so grouping happens above foca. `rev` lets a member declared down while alive rejoin at once (finding 5) | Task 4 (`a_restart_at_a_new_address_counts_at_once`, `a_phase_change_reaches_every_peer`) |
| D-M8a.2 | **The members.** Per name, the identity with the highest `(incarnation, rev)` that foca doesn't hold down. This node is always a member. A suspect is a member, flagged `suspect` | A restarted node's old identity lingers as Suspect until it is declared down (finding 4); grouping hides it. Suspects stay in the ring (D-11) | Task 4 (`the_members_are_each_names_latest_live_identity`, `a_killed_node_is_suspect_then_gone`) |
| D-M8a.3 | **The MAC.** A packet is `payload ‖ blake3::keyed_hash(key, payload)`, with the key derived as in Global Constraints. A packet whose MAC doesn't match is dropped and counted; a warning is logged at most once a minute (`WARN_EVERY`) | Every node of a cluster reads the anchor, and no config carries a secret (Q-M8-2). The MAC is checked before foca decodes anything. One warning a minute keeps a misdirected sender visible without flooding the log | Task 3 (`a_sealed_packet_opens_and_a_changed_one_doesnt`, `another_anchor_is_another_key`), Task 4 (`a_node_with_another_key_cant_join`) |
| D-M8a.4 | **foca's settings, from config §6.5.1**, for `n` members. They are recomputed (`set_config`) whenever `n` changes. The mapping is in the table after this one | The spec's timers keep their meaning. Announcing to down members is what heals a partition (finding 6). The suspicion window grows with the cluster, as memberlist's did | Task 4 (`foca_follows_the_spec_formulas`) |
| D-M8a.5 | **The wire.** `trait Wire { fn send(&self, to, packet) }`. `Udp` binds one socket per bind address; each socket has a writer task and a queue (`INBOX` 1024), and packets are read into one inbox. A full queue drops the packet: gossip tolerates loss. In tests, `test_support::Hub` delivers between in-memory ports and can cut and heal links | A UDP `try_send_to` right after the bind returned `WouldBlock` (measured on macOS). The hub makes the gossip, placement and cluster tests deterministic on the paused clock | Task 3 (`packets_cross_udp_on_loopback`, `the_hub_delivers_until_cut_and_after_a_heal`), Task 11 (`an_address_taken_is_an_error_naming_it`) |
| D-M8a.6 | **The ring.** `Ring::new(nodes, tokens)` holds `tokens` points per node, sorted (ties ordered by name). `owner(id)` is the node of the first point at or after `xxh3_64(id)`, wrapping. Member order and duplicates don't matter. The placement of a fixed set of ids is pinned in a test | Clustering §3.1 (B): every node computes byte-identical rings from the same members. Pinning the placement catches a hash or order change that would move every pipeline at an upgrade | Task 2 (`the_placement_is_pinned`, `the_member_order_and_duplicates_dont_matter`, `a_join_or_a_leave_moves_only_what_it_must`) |
| D-M8a.7 | **Placement (D-11).** A node is eligible when it is a member in phase `Active`. A node that stops being eligible leaves the ring at once (gone, `Draining`, `Leaving`). A node that becomes eligible joins after `rebalance_stabilization` of continuous eligibility: a flap restarts its wait. An empty ring takes the eligible peers at once, and this node only when no peer is eligible. **A `Draining` node is off the ring at once.** Research §3.3 kept it on "until its handoffs complete"; the drain itself is the controller's (D-M8a.13) | Removals at once are safe: the lease, not the ring, keeps a pipeline single-run, so a new owner just waits for the lease. Delayed additions stop a flapping joiner from pulling pipelines. A draining node off the ring lets each owner-to-be claim at once, and the drain releases each lease when its worker stops | Task 5 (all four tests) |
| D-M8a.8 | **The join.** A node starts `Joining`. With seeds, it waits for a first peer, at most `JOIN_WAIT` (5 s), then turns `Active`. Without seeds it turns `Active` at once | A seeded node that turned Active alone would find its ring empty and take every pipeline into its own ring (D-M8a.7's empty-ring rule). Its claims would then wait on leases that the others hold, and flap when the peers arrive. Waiting briefly for a peer gives it the real ring, with itself added after the stabilization | Task 9 (`a_node_with_seeds_waits_for_a_peer_then_is_active`, `a_node_without_seeds_is_active_at_once`) |
| D-M8a.9 | **The reconciler.** It runs as a task and recomputes placement on three triggers: a members change, the next stabilization due, and every `lease_renewal_interval` (the resync). A pipeline's target is `Own` exactly when the ring's owner is this node. It sets `data_avalanche_cluster_members` and the new gauge `data_avalanche_ring_generation` (bumped when the ring changes) | Level-triggered with a resync (Clustering §20.1), so a lost wake-up costs one interval at most | Task 6 (`a_joiner_gets_its_pipelines_after_the_stabilization`, `the_settings_come_from_the_cluster`) |
| D-M8a.10 | **The hand-off cap.** When a running controller's target turns `DontOwn`, it waits for a permit from `ControllerDeps.handoffs` (`max_concurrent_handoffs`) and drains while holding it. A controller with no worker releases its lease at once, without a permit. A shutdown (the target channel closed) isn't capped | Clustering §8.5: a rebalance must not stop every worker at once. A pipeline with no worker has nothing to drain. A leaving node (D-M8a.13) caps its hand-offs through the targets; a shutdown is the end of the process | Task 6 (`the_hand_off_cap_drains_one_pipeline_at_a_time`: cap 1 takes 10–11 s for two 5 s drains, cap 2 takes 5–6 s) |
| D-M8a.11 | **Supervision (DA-OPS-7).** A controller ends only when its target channel closes, so one that ended otherwise panicked. The reconciler logs it at `ERROR` and starts it again on the same status channel, at the current target, at its next pass (the resync at the latest). The new controller takes the lease over | M6b's reconciler looked at its tasks only at shutdown. After a panic the pipeline went unmanaged until its lease expired, with no takeover on a single node | Task 6 (`a_controller_that_panics_is_started_again`) |
| D-M8a.12 | **A drop is a crash.** Dropping `ClusterReconciler` (or `Gossip`, or `Udp`) aborts its tasks without releasing anything; the leases expire. Only `shutdown` and `leave` release | The in-process cluster tests simulate a crash by a drop, and the code has one path for a real crash | Task 6 (`a_reconciler_dropped_is_a_crash_and_its_leases_stay`), Task 10 (`a_crashed_nodes_pipelines_are_taken_over_after_its_lease`) |
| D-M8a.13 | **The leave.** With peers: phase `Draining`, then wait until every controller is on standby (`released()`), then phase `Leaving`, the reconciler's shutdown, and the gossip's leave. A lone node: the reconciler's shutdown, then the leave | Draining takes the node off every ring (D-M8a.7), so the owners-to-be claim while this node drains under the hand-off cap. The gossip's leave makes the others see the node gone at once, not after a suspicion | Task 7 (`a_leave_hands_every_pipeline_off_without_a_lease_wait`, `a_lone_node_leaves_by_releasing_its_leases`) |
| D-M8a.14 | **A lake ahead of the lease (DA-CLUS-14).** A worker fenced by a table's `current_epoch` makes the controller claim again with `Claim::at_least(current_epoch + 1)`. The claim takes over its own lease at `max(stored + 1, floor)` (`Leases::take_over_at`), and keeps the floor until that takeover lands. Another holder's lease is untouched by the floor. If the worker is fenced again after a jumped takeover, the pipeline goes to `ERROR` ("another lease store …", no code) and keeps its lease | A moved, restored or deleted `globals.state` leaves the lake ahead of the lease store. M6b climbed one worker run per epoch, and with two live lease stores on one lake the climb would defeat the fence. One jump recovers the restored case; a second fence can only be another live store | Task 8 (all four tests) |
| D-M8a.15 | **Discovery.** `Seeds::Static(host:port list)` or `Seeds::Dns { name, port: advertise_port }`, from `discovery`. The node's own addresses are left out. The seeds are asked at start and then every `REDISCOVER` (5 s) while the node has no peer | Clustering §10.2 (D-4). A headless Service's records include this pod, and a node that lost every peer (a partition, the first pod of a StatefulSet) must find the others again | Task 9 (`static_seeds_resolve_and_leave_this_node_out`, `dns_seeds_are_every_address_at_the_advertise_port`, `a_node_without_peers_asks_its_seeds_again`) |
| D-M8a.16 | **The advertised address**, in this order: `advertise_addr` (an IP, or a name resolved once at start); else the first `bind_addr` that isn't a wildcard; else the host's first private IPv4; else `ClusterError::AdvertiseAddr`, naming the key. The port is `advertise_port` | foca replies to the address in the sender's identity (finding 7), so a wildcard would be unreachable. A pod has one private address; a laptop's choice should be explicit | Task 9 (`the_advertised_address_is_the_configured_one_else_a_bind_address_else_a_private_one`, `no_private_address_and_no_setting_is_an_error_naming_the_key`) |
| D-M8a.17 | **Typed errors.** `ClusterError { NodeName, AdvertiseAddr, Bind { addr, source } }`. The binary maps a bind error to `Fatal::Startup` and the others to `Fatal::Config` | A port taken or an address missing must name what to set | Task 9, Task 11 (`an_address_taken_is_an_error_naming_it`) |
| D-M8a.18 | **Per-node values come from the environment (D-3).** A node runs the cluster's config generation, resolved with its own environment, not its own file. So a value that differs per node (`node_name`, `advertise_addr`, ports) must be a `${VAR}` in the one config. The E2E harness writes one `config.yaml` and gives each node `E2E_NODE`, `E2E_API_PORT`, `E2E_GOSSIP_PORT` and `E2E_ADVERTISE_PORT`. A `${…}` inside a flow map is quoted | Measured: a second node with its own file adopted the first node's generation, and exited "Address already in use" on that node's port. The config spec (§6.5.1, this plan's patch) and the operator docs (M11) must say so | Task 11 (the lifecycle E2E's two nodes), Task 12 |
| D-M8a.19 | **The config keys foca doesn't use are removed**, and are now unknown keys: `packet_dial_timeout`, `packet_write_timeout`, `compression_enabled`, `lifeguard_enabled`, `tls` and `dead_node_reclaim_time`. V-054 is retired. V-122: `gossip_interval`, `probe_interval`, `probe_timeout` and `push_pull_interval` above zero, `gossip_nodes` and `retransmit_factor` at least 1, `probe_timeout` below `probe_interval` | foca has no TCP, compression, Lifeguard or TLS. A newer identity wins at once, and a down member is remembered so that a partition can heal, so there is nothing to reclaim. A key that does nothing must not look as if it does | Task 1 (`the_memberlist_only_keys_are_unknown`, the V-122 rows) |
| D-M8a.20 | **The E2E cluster.** Three processes on loopback, each gossiping on its own port and advertising a `Forwarder`'s port in front of it. `block(&[ports])` drops packets whose source is the other side's ports; `heal()` lets them through again. A scenario has settled when every live node's holders equal the ring's placement over the live nodes, and the members and owned metrics agree. No double commit: each table's commits carry `data-avalanche.owner_epoch` in non-decreasing order. The pause scenario waits up to 150 s for the new holder to converge: on PostgreSQL the paused node's walsender keeps the slot until `wal_sender_timeout` (60 s), and the new holder's next busy-slot retry comes at most a backoff step (60 s) later | foca replies to the advertised address, so a forwarder sees every packet sent to its node (finding 7). Settling on the ring, rather than on a count, catches a scenario that would pass before its hand-offs | Task 12 (the four scenarios) |
| D-M8a.21 | **The in-process cluster's invariant.** While a Task 10 scenario waits for its placement (`settles`), it checks at every 10 ms of the test clock that no pipeline is `Starting` or `Running` on two nodes | This is the safety property the ring cannot give and the lease must | Task 10 (all four tests) |

**foca's settings (D-M8a.4)**, for `n` members:

| foca | From |
|---|---|
| `probe_period` | `probe_interval` |
| `probe_rtt` | `probe_timeout` |
| `num_indirect_probes` | 3 |
| `max_transmissions` | `retransmit_factor · ceil(log10(n + 1))`, clamped to 1..255 |
| `suspect_to_down_after` | `4 · max(1, log10 n) · probe_interval` |
| `remove_down_after` | 24 h |
| `max_packet_size` | 1400 |
| `notify_down_members` | true |
| `periodic_gossip` | (`gossip_interval`, `gossip_nodes`) |
| `periodic_announce` | (`push_pull_interval`, 1) |
| `periodic_announce_to_down_members` | (`push_pull_interval`, 2) |

**Already in place** (no task): M6b's startup checks the anchor (V-062, V-076, V-094:
`check_cluster_anchor`). `max_concurrent_takeovers` caps Phase 0 (M6b's `starts` semaphore, with
`start_jitter`). Task 6 moves M6b's test of it (`one_phase_0_at_a_time_holds_the_other_start`) onto
the ring: `q`'s lease is held by another node at first, and `p`'s preflight retries longer, so the
order is fixed on the paused clock.

**Measured while planning** (2026-10-20…21, recorded in the tasks):

- The probe gate (`prototypes/cluster-capability`). memberlist 0.8.5's `by_id` panics after
  reaping (C13). foca 2.0 joins 3 nodes over UDP in 0.21 s. It sees a kill as suspect at 1.8 s and
  down at 5.8 s, and a restart at a new address at once (0.02–0.23 s). A partition is seen in
  6.8 s and heals in 1.0 s.
- A UDP socket's `try_send_to` right after `bind` returned `WouldBlock` on macOS, hence a writer
  task per socket (D-M8a.5).
- An encoded identity is 85 bytes (probe F8), well inside a 1400-byte packet.
- The hand-off cap, two pipelines whose drains each hang 5 s: cap 1 finishes in 10–11 s, cap 2
  in 5–6 s (paused clock).
- Stress runs of the cluster crate as planned (unchanged since): the gossip's tests 40 of 40 runs,
  the crate's whole suite 30 of 30.
- D-3: a second E2E node started with its own file adopted the first node's generation, and exited
  "Address already in use" on that node's API port. With the per-node values in `${E2E_*}`, the
  E2E suite after Task 11 passes: PostgreSQL 18, 33 of 33; `mysql-84`, 33 of 33.
- The cluster scenarios (Task 12), in the order they were found:
  - The first run failed two scenarios. The partition scenario took its baseline before the
    hand-offs had settled, which is now D-M8a.20's settling rule. The pause scenario indexed the
    lease holders while the paused node's lease was being taken over; its wait now reads "no
    holder yet" as "not yet".
  - After those fixes, all four passed in 10 of 12 runs on PostgreSQL 18 (139–196 s each), and in
    the one run on `mysql-84`. The 2 failures were the pause scenario: the new holder converged
    past the 60 s wait. The paused node's walsender held the slot ("active for PID …") until
    `wal_sender_timeout`, and the new holder's busy-slot retries, backing off toward 60 s, missed
    the deadline.
  - The pause scenario now waits 150 s (D-M8a.20). Ten runs of it alone pass; in four timed ones
    the new holder converged 53–91 s after the takeover. The wait is PostgreSQL's slot, and the
    finding is DA-SRC-28.
- The whole E2E suite on the final tree: PostgreSQL 18, 37 of 37 (469 s); `mysql-84`, 37 of 37
  (381 s).
- foca's default cadence announces to down members every 65 s; ours is `push_pull_interval` (30 s
  by default). The E2E sets 5 s, so a healed partition rejoins within seconds.

**Documents this plan changes:**

- Applied at plan approval, in an `infra:` and a `gap:` commit:
  - Roadmap:
    - §2.1: the membership row (`foca` `=2.0.0`, `rand`, `if-addrs`, `xxhash-rust`).
    - §3.7: the cluster's new interfaces (D-M8a.1…17).
    - §4 M8: split into M8a and M8b (Q-M8-3); the docker-compose 3-node run moves to M11's kind
      smoke (Q-M8-4).
    - §6: R-1 realized (memberlist failed the gate; the swap was contained).
    - §9: D-IMPL-17 for D-M8a.1…21. §9.1: Q-M8-1…4.
  - `Clustering_Architecture_Research.md`:
    - §2: foca, not memberlist.
    - §3.3: Draining is off the ring (D-M8a.7); there is no `dead_node_reclaim_time`.
    - §3.6: no Lifeguard. SWIM accuracy stays a latency concern (§8.4).
    - §13: the modules as built.
  - `doc/v1.0/README.md`: the M8a row.
  - `CDC_Design_Gap_Analysis_v1.md`: a dated section for M8a's plan, with two new gaps:
    - DA-SRC-28: after a paused holder, a PostgreSQL takeover waits for `wal_sender_timeout`
      plus a busy-slot backoff;
    - DA-OPS-8: per-node literals in a node's own file are ignored once a generation exists
      (D-M8a.18).
- **Spec edits, which need their own approval** (drafted on scratch copies; the patch is in this
  plan's review message):
  - `Configuration_Specification.md`:
    - §4.4: drop `memberlist.tls` from the TLS fields.
    - §6.5.1: foca over UDP; the MAC paragraph; the removed keys; reworded `bind_port`,
      `probe_*`, `push_pull_interval` and `retransmit_factor`; per-node values through `${VAR}`.
    - Remove §6.5.2.
    - §11: drop `memberlist.tls` from the inert-fields rule.
    - §13: delete V-054; fix V-068's text; add V-122.
    - §14: trim the example.
    - §15: a new item 27.
  - `System-Overview.md`: the cluster-state row (foca), and OQ-API1 (the gossip MAC, not TLS).
- `test-infra/README.md`: the E2E nodes' environment and the cluster scenarios (Task 12).

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-config/src/{raw,walk,build,resolved,registry,diff,mutability}.rs`, `rules/{auth,domain}.rs` | `globals.cluster.memberlist` without the removed keys; V-122; V-054 retired |
| `crates/data-avalanche-cluster/src/ring.rs` (new) | `Ring`: points per node, owners |
| `crates/data-avalanche-cluster/src/wire.rs` (new) | `GossipKey` (the MAC), `Wire`, `Udp` |
| `crates/data-avalanche-cluster/src/gossip.rs` (new) | `NodeIdentity`, `NodeMeta`, `Phase`, `Members`, `GossipSettings`, `Gossip` (the foca actor), `Announcer` |
| `crates/data-avalanche-cluster/src/placement.rs` (new) | `Placement`: eligibility, stabilization, the ring generation |
| `crates/data-avalanche-cluster/src/reconciler.rs` | `ClusterReconciler` on the members: targets, supervision, `released`, `ReconcilerSettings` |
| `crates/data-avalanche-cluster/src/controller.rs` | The hand-off permit; `reporting`; the fence's epoch carried into the claim |
| `crates/data-avalanche-cluster/src/claim.rs` | `Claim::at_least`, `Claimed::Mine { jumped }` |
| `crates/data-avalanche-cluster/src/node.rs` (new) | `join`, `leave`, `JOIN_WAIT` |
| `crates/data-avalanche-cluster/src/discovery.rs` (new) | `Seeds`, `discover`, `advertise`, `interfaces` |
| `crates/data-avalanche-cluster/src/error.rs` (new) | `ClusterError` |
| `crates/data-avalanche-cluster/src/test_support.rs` | `Hub`, `Port`, `TestCluster`, `TestNode`, `settles` |
| `crates/data-avalanche-cluster/src/cluster_tests.rs` (new) | The three-node in-process scenarios |
| `crates/data-avalanche-state/src/lease.rs` | `Leases::take_over_at` |
| `crates/data-avalanche-telemetry/src/lib.rs` | `set_ring_generation`, `data_avalanche_ring_generation` |
| `crates/data-avalanche/src/{startup,run}.rs` | The anchor's key out of startup; the node's gossip, discovery, join, reconciler and leave |
| `crates/data-avalanche/tests/e2e/harness.rs` | One config with per-node `${E2E_*}` values; spawning nodes with ports; signals |
| `crates/data-avalanche/tests/e2e/forwarder.rs` (new) | The UDP forwarder |
| `crates/data-avalanche/tests/e2e/cluster.rs` (new) | The multi-node scenarios |
| `test-infra/README.md` | The E2E nodes' environment, the cluster scenarios |

---

### Task 1: `globals.cluster.memberlist` for foca (`data-avalanche-config`)

**Files:**
- Modify: `crates/data-avalanche-config/src/build.rs`
- Modify: `crates/data-avalanche-config/src/diff.rs`
- Modify: `crates/data-avalanche-config/src/mutability.rs`
- Modify: `crates/data-avalanche-config/src/raw.rs`
- Modify: `crates/data-avalanche-config/src/registry.rs`
- Modify: `crates/data-avalanche-config/src/resolved.rs`
- Modify: `crates/data-avalanche-config/src/rules/auth.rs`
- Modify: `crates/data-avalanche-config/src/rules/domain.rs`
- Modify: `crates/data-avalanche-config/src/walk.rs`
- Modify (test): `crates/data-avalanche-config/tests/fixtures/spec_example.yaml`
- Modify (test): `crates/data-avalanche-config/tests/load.rs`
- Modify (test): `crates/data-avalanche-config/tests/rules/auth.rs`
- Modify (test): `crates/data-avalanche-config/tests/rules/domain.rs`
- Modify (test): `crates/data-avalanche-config/tests/support/mod.rs`

**Interfaces:**
- Consumes: M1's config model (`raw`, `walk`, `build`, `resolved`, `registry`, the rule modules).
- Produces:
  - `data_avalanche_config::Memberlist` without `tls`, `lifeguard_enabled`, `compression_enabled`, `packet_dial_timeout`, `packet_write_timeout` and `dead_node_reclaim_time`. It keeps `join_members: Vec<HostPort>`, `node_name: Option<String>`, `advertise_addr: String`, `advertise_port: u16`, `bind_addr: Vec<IpAddr>`, `bind_port: u16`, `gossip_interval`, `gossip_nodes: u32`, `probe_interval`, `probe_timeout`, `push_pull_interval: Duration` and `retransmit_factor: u32`.
  - Rule `V-122` (error, config scope); `V-054` retired.

D-M8a.19. foca has no TCP stream, compression, Lifeguard or TLS, and it accepts a newer identity at once, so the keys memberlist used for these do nothing now. A key that does nothing must not look as if it does, so the six keys become unknown keys and a config that still sets one is refused at parse time. V-054 (gossip TLS needs `cert` and `key`) goes with `tls`; the registry test lists it among the retired IDs. V-122 keeps the gossip's timers usable: a zero interval would spin the actor, and a probe timeout at or past the interval would overlap probes. The spec example (§14) and the fixtures lose the gossip TLS material.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/src/build.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/build.rs b/crates/data-avalanche-config/src/build.rs
index c8648d5..dd6be8c 100644
--- a/crates/data-avalanche-config/src/build.rs
+++ b/crates/data-avalanche-config/src/build.rs
@@ -1467,7 +1467,7 @@ mod tests {
     }
 
     /// V-068 / §11 Phase 2: `hive.tls.ca` is only in effect when `tls.enabled` is `true`, the same
-    /// gate `api.tls`/`memberlist.tls` use. Unreachable via `load_str` today (V-115 refuses `hive`
+    /// gate `api.tls` uses. Unreachable via `load_str` today (V-115 refuses `hive`
     /// in Phase 1), so exercised directly against `hive()`.
     #[test]
     fn the_derived_server_id_is_stable_and_in_the_high_range() {
```

Change `crates/data-avalanche-config/src/diff.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/diff.rs b/crates/data-avalanche-config/src/diff.rs
index c478a47..5cf875c 100644
--- a/crates/data-avalanche-config/src/diff.rs
+++ b/crates/data-avalanche-config/src/diff.rs
@@ -315,8 +315,6 @@ pipelines:
 
     fn spec_example(files: &Files) -> (String, BTreeMap<String, String>) {
         files.write("api.crt", cert());
-        files.write("gossip.crt", cert());
-        files.write("gossip-ca.crt", cert());
         let creds = files.write(
             "events-db.json",
             r#"{"user": "events", "password": "events-pw"}"#,
@@ -325,7 +323,6 @@ pipelines:
         let env = env(&[
             ("ACCOUNT_ID", "123456789012"),
             ("API_TLS_KEY", key()),
-            ("GOSSIP_TLS_KEY", key()),
             ("CATALOG_CLIENT_ID", "cdc-client"),
             ("CATALOG_CLIENT_SECRET", "client-secret"),
             ("ORDERS_DB_PASSWORD", "orders-pw"),
@@ -351,7 +348,7 @@ pipelines:
             .replace(
                 "globals:\n  state: { bucket: app-state }",
                 &format!(
-                    "globals:\n  cloud: azure\n  state: {{ bucket: app-state, prefix: app }}\n  azure: {{ account_name: lakeacct }}\n  api:\n    auth: basic\n    user: admin\n    password: x\n    tls: {{ enabled: true, cert: {c:?}, key: {k:?} }}\n  cluster:\n    memberlist:\n      join_members: [\"a:7946\"]\n      tls: {{ enabled: true, cert: {c:?}, key: {k:?}, ca: {c:?} }}\n  tracing: {{ otlp_endpoint: \"http://otel:4317\" }}",
+                    "globals:\n  cloud: azure\n  state: {{ bucket: app-state, prefix: app }}\n  azure: {{ account_name: lakeacct }}\n  api:\n    auth: basic\n    user: admin\n    password: x\n    tls: {{ enabled: true, cert: {c:?}, key: {k:?} }}\n  cluster:\n    memberlist:\n      join_members: [\"a:7946\"]\n  tracing: {{ otlp_endpoint: \"http://otel:4317\" }}",
                     c = pem_cert,
                     k = pem_key
                 ),
```

Change `crates/data-avalanche-config/src/mutability.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/mutability.rs b/crates/data-avalanche-config/src/mutability.rs
index c9a72fe..7a7ce23 100644
--- a/crates/data-avalanche-config/src/mutability.rs
+++ b/crates/data-avalanche-config/src/mutability.rs
@@ -98,7 +98,7 @@ mod tests {
         assert_eq!(classify(&["globals", "cluster", "tokens"]), Some(Proc));
         assert_eq!(classify(&["globals", "cluster", "drain_timeout"]), Some(R));
         assert_eq!(
-            classify(&["globals", "cluster", "memberlist", "tls", "cert", "text"]),
+            classify(&["globals", "cluster", "memberlist", "bind_port"]),
             Some(Proc)
         );
         assert_eq!(classify(&["pipelines", "p", "source", "host"]), Some(Pr));
```

Change `crates/data-avalanche-config/src/raw.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/raw.rs b/crates/data-avalanche-config/src/raw.rs
index 464fbff..e2342a0 100644
--- a/crates/data-avalanche-config/src/raw.rs
+++ b/crates/data-avalanche-config/src/raw.rs
@@ -444,8 +444,8 @@ mod tests {
         let globals = raw.globals.unwrap();
         let cluster = globals.cluster.unwrap();
         assert_eq!(cluster.lease_duration.unwrap().text(), "15s");
-        let tls = cluster.memberlist.unwrap().tls.unwrap();
-        assert_eq!(tls.key.unwrap().text(), "${GOSSIP_TLS_KEY}");
+        let memberlist = cluster.memberlist.unwrap();
+        assert_eq!(memberlist.gossip_interval.unwrap().text(), "200ms");
         let defaults = raw.defaults.unwrap();
         let rest = defaults
             .sink
@@ -493,7 +493,6 @@ mod tests {
         "globals: {log: {bogus: 1}}",
         "globals: {cluster: {bogus: 1}}",
         "globals: {cluster: {memberlist: {bogus: 1}}}",
-        "globals: {cluster: {memberlist: {tls: {bogus: 1}}}}",
         "globals: {tracing: {bogus: 1}}",
         "globals: {azure: {bogus: 1}}",
         "defaults: {bogus: 1}",
@@ -538,6 +537,29 @@ mod tests {
         }
     }
 
+    /// §6.5.1 as of M8a (Q-M8-1): the gossip is foca over UDP, so memberlist's TCP, compression,
+    /// Lifeguard and TLS keys are gone; a config that still sets one is refused, not ignored.
+    #[test]
+    fn the_memberlist_only_keys_are_unknown() {
+        for key in [
+            "tls: {enabled: false}",
+            "lifeguard_enabled: true",
+            "compression_enabled: true",
+            "packet_dial_timeout: 5s",
+            "packet_write_timeout: 5s",
+            "dead_node_reclaim_time: 30s",
+        ] {
+            let case = format!("globals: {{cluster: {{memberlist: {{{key}}}}}}}");
+            let err = parse_document::<RawConfig>(&case).expect_err(&case);
+            assert_eq!(err.rule, PARSE, "{case}");
+            assert!(
+                err.message.contains("unknown field"),
+                "{case}: {}",
+                err.message
+            );
+        }
+    }
+
     #[test]
     fn old_draft_spellings_are_unknown_keys() {
         // §15: the draft's kebab-case `ssl-mode` and the pre-multi-sink pipeline-level `catalog`.
```

Change `crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
index 574d45b..3c94927 100644
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1046,8 +1046,8 @@ mod tests {
             RULES.windows(2).all(|w| w[0].id < w[1].id),
             "sorted, no duplicates"
         );
-        // IDs retired by the spec (§15 item 18) or never allocated in it.
-        for gone in ["V-024", "V-031", "V-032", "V-039", "V-090"] {
+        // IDs retired by the spec (§15 items 18 and 27) or never allocated in it.
+        for gone in ["V-024", "V-031", "V-032", "V-039", "V-054", "V-090"] {
             assert!(rule_meta(gone).is_none(), "{gone}");
         }
         assert_eq!(
```

Change `crates/data-avalanche-config/tests/fixtures/spec_example.yaml`:

```diff
diff --git a/crates/data-avalanche-config/tests/fixtures/spec_example.yaml b/crates/data-avalanche-config/tests/fixtures/spec_example.yaml
index c451530..b758451 100644
--- a/crates/data-avalanche-config/tests/fixtures/spec_example.yaml
+++ b/crates/data-avalanche-config/tests/fixtures/spec_example.yaml
@@ -35,13 +35,6 @@ globals:
       probe_interval: 1s
       probe_timeout: 500ms
       push_pull_interval: 30s
-      dead_node_reclaim_time: 30s
-      compression_enabled: true
-      tls:
-        enabled: true
-        cert: /etc/cdc/tls/gossip.crt
-        key: ${GOSSIP_TLS_KEY}
-        ca: /etc/cdc/tls/gossip-ca.crt
   tracing:
     otlp_endpoint: http://otel-collector.observability:4317
     sample_ratio: 0.05
```

Change `crates/data-avalanche-config/tests/load.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/load.rs b/crates/data-avalanche-config/tests/load.rs
index 5143bd8..18918e1 100644
--- a/crates/data-avalanche-config/tests/load.rs
+++ b/crates/data-avalanche-config/tests/load.rs
@@ -24,7 +24,7 @@ fn spec_example_resolves() {
     assert_eq!(tls.key.origin, Origin::Inline);
     let cluster = &cfg.globals.cluster;
     assert_eq!(cluster.discovery, Discovery::Dns);
-    assert!(cluster.memberlist.tls.as_ref().unwrap().ca.is_some());
+    assert_eq!(cluster.memberlist.join_members.len(), 1);
     assert_eq!(cfg.globals.tracing.sample_ratio, 0.05);
     assert_eq!(
         cfg.pipelines.keys().collect::<Vec<_>>(),
```

Change `crates/data-avalanche-config/tests/rules/auth.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/auth.rs b/crates/data-avalanche-config/tests/rules/auth.rs
index 4abb9de..f7ccb13 100644
--- a/crates/data-avalanche-config/tests/rules/auth.rs
+++ b/crates/data-avalanche-config/tests/rules/auth.rs
@@ -138,15 +138,6 @@ pub fn cases() -> Vec<Case> {
                 json!({"enabled": true, "cert": cert(), "key": key()}),
             )
         }),
-        case("V-054", Error, "gossip tls without cert", || {
-            pg_delta().set(
-                "/globals/cluster/memberlist/tls",
-                json!({"enabled": true, "key": key()}),
-            )
-        }),
-        case("V-054", Silent, "gossip tls disabled", || {
-            pg_delta().set("/globals/cluster/memberlist/tls", json!({"enabled": false}))
-        }),
         case("V-099", Error, "unknown role", || {
             pg_delta().set("/globals/api", json!({
                 "auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"},
```

Change `crates/data-avalanche-config/tests/rules/domain.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/domain.rs b/crates/data-avalanche-config/tests/rules/domain.rs
index f426e12..cd98df0 100644
--- a/crates/data-avalanche-config/tests/rules/domain.rs
+++ b/crates/data-avalanche-config/tests/rules/domain.rs
@@ -215,6 +215,36 @@ pub fn cases() -> Vec<Case> {
         case("V-074", Silent, "takeovers 1", || {
             pg_delta().set("/globals/cluster/max_concurrent_takeovers", json!(1))
         }),
+        case("V-122", Error, "gossip interval 0s", || {
+            pg_delta().set("/globals/cluster/memberlist/gossip_interval", json!("0s"))
+        }),
+        case("V-122", Error, "push pull interval 0s", || {
+            pg_delta().set(
+                "/globals/cluster/memberlist/push_pull_interval",
+                json!("0s"),
+            )
+        }),
+        case("V-122", Error, "gossip nodes 0", || {
+            pg_delta().set("/globals/cluster/memberlist/gossip_nodes", json!(0))
+        }),
+        case("V-122", Error, "retransmit factor 0", || {
+            pg_delta().set("/globals/cluster/memberlist/retransmit_factor", json!(0))
+        }),
+        case("V-122", Error, "probe timeout at the interval", || {
+            pg_delta().set("/globals/cluster/memberlist/probe_timeout", json!("1s"))
+        }),
+        case(
+            "V-122",
+            Error,
+            "probe interval below the default timeout",
+            || pg_delta().set("/globals/cluster/memberlist/probe_interval", json!("400ms")),
+        ),
+        case("V-122", Silent, "probe timeout below the interval", || {
+            pg_delta().set(
+                "/globals/cluster/memberlist",
+                json!({"probe_interval": "2s", "probe_timeout": "1500ms", "gossip_nodes": 1}),
+            )
+        }),
         case("V-097", Error, "ratio 1.5", || {
             pg_delta().set(
                 "/globals/tracing",
```

Change `crates/data-avalanche-config/tests/support/mod.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/support/mod.rs b/crates/data-avalanche-config/tests/support/mod.rs
index e335766..99b6a5f 100644
--- a/crates/data-avalanche-config/tests/support/mod.rs
+++ b/crates/data-avalanche-config/tests/support/mod.rs
@@ -95,8 +95,6 @@ impl Files {
 /// references set.
 pub fn spec_example(files: &Files) -> (String, BTreeMap<String, String>) {
     files.write("api.crt", cert());
-    files.write("gossip.crt", cert());
-    files.write("gossip-ca.crt", cert());
     let creds = files.write(
         "events-db.json",
         r#"{"user": "events", "password": "events-pw"}"#,
@@ -105,7 +103,6 @@ pub fn spec_example(files: &Files) -> (String, BTreeMap<String, String>) {
     let env = env(&[
         ("ACCOUNT_ID", "123456789012"),
         ("API_TLS_KEY", key()),
-        ("GOSSIP_TLS_KEY", key()),
         ("CATALOG_CLIENT_ID", "cdc-client"),
         ("CATALOG_CLIENT_SECRET", "client-secret"),
         ("ORDERS_DB_PASSWORD", "orders-pw"),
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`

Expected: FAIL. The library's tests run first, and 2 of them fail: the removed keys still parse (`the_memberlist_only_keys_are_unknown`), and V-054 is still registered (`has_every_spec_rule_once_in_order`). Cargo stops at the library. The fixtures, without the TLS material, fail in the integration suites once the library passes.

```text
test registry::tests::has_every_spec_rule_once_in_order ... FAILED
test raw::tests::the_memberlist_only_keys_are_unknown ... FAILED
thread 'registry::tests::has_every_spec_rule_once_in_order' (3560088) panicked at crates/data-avalanche-config/src/registry.rs:1051:13:
thread 'raw::tests::the_memberlist_only_keys_are_unknown' (3560086) panicked at crates/data-avalanche-config/src/raw.rs:553:58:
test result: FAILED. 81 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
error: test failed, to rerun pass `-p data-avalanche-config --lib`
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-config/src/build.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/build.rs b/crates/data-avalanche-config/src/build.rs
index dd6be8c..15d284a 100644
--- a/crates/data-avalanche-config/src/build.rs
+++ b/crates/data-avalanche-config/src/build.rs
@@ -530,21 +530,6 @@ fn memberlist(m: &RawMemberlist, at: &At, ctx: &mut Ctx) -> Option<Memberlist> {
         some => list(ctx, some, at, "bind_addr"),
     };
     let bind_port = or(ctx, &m.bind_port, at, "bind_port", 7946);
-    let packet_dial_timeout = or(
-        ctx,
-        &m.packet_dial_timeout,
-        at,
-        "packet_dial_timeout",
-        secs(5),
-    );
-    let packet_write_timeout = or(
-        ctx,
-        &m.packet_write_timeout,
-        at,
-        "packet_write_timeout",
-        secs(5),
-    );
-    let compression_enabled = or(ctx, &m.compression_enabled, at, "compression_enabled", true);
     let gossip_interval = or(
         ctx,
         &m.gossip_interval,
@@ -561,13 +546,6 @@ fn memberlist(m: &RawMemberlist, at: &At, ctx: &mut Ctx) -> Option<Memberlist> {
         "probe_timeout",
         Duration::from_millis(500),
     );
-    let dead_node_reclaim_time = or(
-        ctx,
-        &m.dead_node_reclaim_time,
-        at,
-        "dead_node_reclaim_time",
-        secs(30),
-    );
     let push_pull_interval = or(
         ctx,
         &m.push_pull_interval,
@@ -576,44 +554,6 @@ fn memberlist(m: &RawMemberlist, at: &At, ctx: &mut Ctx) -> Option<Memberlist> {
         secs(30),
     );
     let retransmit_factor = or(ctx, &m.retransmit_factor, at, "retransmit_factor", 4);
-    let lifeguard_enabled = or(ctx, &m.lifeguard_enabled, at, "lifeguard_enabled", true);
-    let tls = m.tls.clone().unwrap_or_default();
-    let tls_at = at.key("tls");
-    let enabled = or(ctx, &tls.enabled, &tls_at, "enabled", false);
-    // §11 Phase 2 (G4): `cert`/`key`/`ca` are only in effect when `tls.enabled` is `true` — the PEM
-    // itself is parsed (and any file read) only then — but a present leaf still gets `${VAR}`
-    // substituted either way (V-063).
-    //
-    // G7: when `enabled` itself can't be resolved (`None`), `tls_in_effect` falls back to `false`
-    // — `enabled`'s own spec default — so this reads as "not in effect" either way; the load
-    // already fails on `enabled`'s own coercion rule regardless of which branch runs here.
-    let tls_in_effect = enabled == Some(true);
-    let cert = if tls_in_effect {
-        pem(ctx, &tls.cert, &tls_at, "cert", PemKind::Certificates)
-    } else {
-        touch(ctx, &tls.cert, &tls_at, "cert");
-        Some(None)
-    };
-    let key = if tls_in_effect {
-        pem(ctx, &tls.key, &tls_at, "key", PemKind::PrivateKey)
-    } else {
-        touch(ctx, &tls.key, &tls_at, "key");
-        Some(None)
-    };
-    let ca = if tls_in_effect {
-        pem(ctx, &tls.ca, &tls_at, "ca", PemKind::Certificates)
-    } else {
-        touch(ctx, &tls.ca, &tls_at, "ca");
-        Some(None)
-    };
-    let tls = match enabled? {
-        true => Some(MemberlistTls {
-            cert: cert??,
-            key: key??,
-            ca: ca?,
-        }),
-        false => None,
-    };
     Some(Memberlist {
         join_members: join_members?,
         node_name: node_name?,
@@ -621,18 +561,12 @@ fn memberlist(m: &RawMemberlist, at: &At, ctx: &mut Ctx) -> Option<Memberlist> {
         advertise_port: advertise_port?,
         bind_addr: bind_addr?,
         bind_port: bind_port?,
-        packet_dial_timeout: packet_dial_timeout?,
-        packet_write_timeout: packet_write_timeout?,
-        compression_enabled: compression_enabled?,
         gossip_interval: gossip_interval?,
         gossip_nodes: gossip_nodes?,
         probe_interval: probe_interval?,
         probe_timeout: probe_timeout?,
-        dead_node_reclaim_time: dead_node_reclaim_time?,
         push_pull_interval: push_pull_interval?,
         retransmit_factor: retransmit_factor?,
-        lifeguard_enabled: lifeguard_enabled?,
-        tls,
     })
 }
 
@@ -1182,7 +1116,7 @@ fn hive(h: &RawHive, at: &At, db: Option<&str>, ctx: &mut Ctx) -> Option<Hive> {
     let enabled = or(ctx, &tls.enabled, &tls_at, "enabled", false);
     // §11 Phase 2 / V-068 (G4): `tls.ca` is only in effect when `tls.enabled` is `true` — the PEM
     // itself is parsed (and any file read) only then — but a present leaf still gets `${VAR}`
-    // substituted either way (V-063). Same gate as `globals.api.tls`/`memberlist.tls` above — in
+    // substituted either way (V-063). Same gate as `globals.api.tls` above — in
     // practice unreachable today (V-115 refuses any `hive` block in Phase 1, before Phase 2 ever
     // reads a file here), but kept for when V-115 lifts.
     let ca = if enabled == Some(true) {
```

Change `crates/data-avalanche-config/src/raw.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/raw.rs b/crates/data-avalanche-config/src/raw.rs
index e2342a0..52184a6 100644
--- a/crates/data-avalanche-config/src/raw.rs
+++ b/crates/data-avalanche-config/src/raw.rs
@@ -121,27 +121,12 @@ pub(crate) struct RawMemberlist {
     pub advertise_port: L<u16>,
     pub bind_addr: Option<Vec<Leaf<IpAddr>>>,
     pub bind_port: L<u16>,
-    pub packet_dial_timeout: L<Duration>,
-    pub packet_write_timeout: L<Duration>,
-    pub compression_enabled: L<bool>,
     pub gossip_interval: L<Duration>,
     pub gossip_nodes: L<u32>,
     pub probe_interval: L<Duration>,
     pub probe_timeout: L<Duration>,
-    pub dead_node_reclaim_time: L<Duration>,
     pub push_pull_interval: L<Duration>,
     pub retransmit_factor: L<u32>,
-    pub lifeguard_enabled: L<bool>,
-    pub tls: Option<RawMemberlistTls>,
-}
-
-#[derive(Debug, Clone, Default, Deserialize)]
-#[serde(deny_unknown_fields)]
-pub(crate) struct RawMemberlistTls {
-    pub enabled: L<bool>,
-    pub cert: L<String>,
-    pub key: L<String>,
-    pub ca: L<String>,
 }
 
 #[derive(Debug, Clone, Default, Deserialize)]
```

Change `crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
index 3c94927..44760f0 100644
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -492,14 +492,6 @@ pub const RULES: &[RuleMeta] = &[
         r#"globals.api.tls.enabled = true"#,
         r#"Requires cert + key."#,
     ),
-    rule(
-        "V-054",
-        Error,
-        L::Config,
-        Config,
-        r#"globals.cluster.memberlist.tls.enabled = true"#,
-        r#"Requires cert + key."#,
-    ),
     rule(
         "V-055",
         Error,
@@ -609,7 +601,7 @@ pub const RULES: &[RuleMeta] = &[
         Error,
         L::ConfigOrPipeline,
         Resolution,
-        r#"TLS material (cert/key/ca across globals.api, memberlist, source.ssl_*, sink.iceberg.catalog.*)"#,
+        r#"TLS material (cert/key/ca across globals.api, source.ssl_*, sink.iceberg.catalog.*)"#,
         r#"Resolved value (inline / file / env) is non-empty and parses as valid PEM (and a private key where a key is expected)."#,
     ),
     rule(
@@ -1028,6 +1020,14 @@ pub const RULES: &[RuleMeta] = &[
         r#"source.mysql.heartbeat"#,
         r#"Between 1s and 4294967s (the server's heartbeat period range)."#,
     ),
+    rule(
+        "V-122",
+        Error,
+        L::Config,
+        Config,
+        r#"globals.cluster.memberlist timers"#,
+        r#"gossip_interval, probe_interval, probe_timeout and push_pull_interval above zero; gossip_nodes and retransmit_factor at least 1; probe_timeout below probe_interval."#,
+    ),
 ];
 
 /// The registry entry for `id` (`"V-007"`).
```

Change `crates/data-avalanche-config/src/resolved.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/resolved.rs b/crates/data-avalanche-config/src/resolved.rs
index dd2fd59..cbb369c 100644
--- a/crates/data-avalanche-config/src/resolved.rs
+++ b/crates/data-avalanche-config/src/resolved.rs
@@ -156,11 +156,6 @@ pub struct Memberlist {
     pub bind_addr: Vec<IpAddr>,
     pub bind_port: u16,
     #[serde(serialize_with = "ser_duration")]
-    pub packet_dial_timeout: Duration,
-    #[serde(serialize_with = "ser_duration")]
-    pub packet_write_timeout: Duration,
-    pub compression_enabled: bool,
-    #[serde(serialize_with = "ser_duration")]
     pub gossip_interval: Duration,
     pub gossip_nodes: u32,
     #[serde(serialize_with = "ser_duration")]
@@ -168,21 +163,8 @@ pub struct Memberlist {
     #[serde(serialize_with = "ser_duration")]
     pub probe_timeout: Duration,
     #[serde(serialize_with = "ser_duration")]
-    pub dead_node_reclaim_time: Duration,
-    #[serde(serialize_with = "ser_duration")]
     pub push_pull_interval: Duration,
     pub retransmit_factor: u32,
-    pub lifeguard_enabled: bool,
-    /// `None` = TLS disabled.
-    pub tls: Option<MemberlistTls>,
-}
-
-#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
-pub struct MemberlistTls {
-    pub cert: Pem,
-    pub key: Pem,
-    /// Presence implies mutual TLS (§6.5.2).
-    pub ca: Option<Pem>,
 }
 
 #[derive(Debug, Clone, PartialEq, Serialize)]
```

Change `crates/data-avalanche-config/src/rules/auth.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/rules/auth.rs b/crates/data-avalanche-config/src/rules/auth.rs
index 0a52643..0c9e4a1 100644
--- a/crates/data-avalanche-config/src/rules/auth.rs
+++ b/crates/data-avalanche-config/src/rules/auth.rs
@@ -61,24 +61,6 @@ pub(crate) fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut
             ctx,
         );
     }
-    if let Some(tls) = g
-        .cluster
-        .as_ref()
-        .and_then(|c| c.memberlist.as_ref())
-        .and_then(|m| m.tls.as_ref())
-        && ctx.peek(&tls.enabled) == Some(true)
-    {
-        // V-054
-        let at = At::config(
-            YamlPath::root()
-                .key("globals")
-                .key("cluster")
-                .key("memberlist")
-                .key("tls"),
-        );
-        require(ctx, "V-054", &at, "cert", &tls.cert, "memberlist TLS");
-        require(ctx, "V-054", &at, "key", &tls.key, "memberlist TLS");
-    }
     for (name, p) in merged {
         let is_iceberg = p
             .sink
```

Change `crates/data-avalanche-config/src/rules/domain.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/rules/domain.rs b/crates/data-avalanche-config/src/rules/domain.rs
index 91cd503..da5343c 100644
--- a/crates/data-avalanche-config/src/rules/domain.rs
+++ b/crates/data-avalanche-config/src/rules/domain.rs
@@ -43,6 +43,71 @@ fn at_least<T: LeafType + PartialOrd + std::fmt::Display>(
     }
 }
 
+/// V-122: timers the gossip can run on. A zero interval would spin it, and a probe that times out
+/// no sooner than the next one starts never fails.
+fn gossip_timers(m: &RawMemberlist, at: &At, ctx: &mut Ctx) {
+    for (leaf, field) in [
+        (&m.gossip_interval, "gossip_interval"),
+        (&m.probe_interval, "probe_interval"),
+        (&m.probe_timeout, "probe_timeout"),
+        (&m.push_pull_interval, "push_pull_interval"),
+    ] {
+        if let Some((v, loc)) = val(ctx, leaf, &at.key(field))
+            && v.is_zero()
+        {
+            ctx.error(
+                "V-122",
+                &at.key(field),
+                loc,
+                format!("{field} must be above zero"),
+            );
+        }
+    }
+    at_least(
+        ctx,
+        "V-122",
+        &m.gossip_nodes,
+        &at.key("gossip_nodes"),
+        1,
+        "gossip_nodes",
+    );
+    at_least(
+        ctx,
+        "V-122",
+        &m.retransmit_factor,
+        &at.key("retransmit_factor"),
+        1,
+        "retransmit_factor",
+    );
+    let interval = m
+        .probe_interval
+        .as_ref()
+        .map_or(Some(Duration::from_secs(1)), |l| {
+            ctx.value(l, &at.key("probe_interval"))
+        });
+    let timeout = m
+        .probe_timeout
+        .as_ref()
+        .map_or(Some(Duration::from_millis(500)), |l| {
+            ctx.value(l, &at.key("probe_timeout"))
+        });
+    if let (Some(i), Some(t)) = (interval, timeout)
+        && t >= i
+    {
+        let loc = m
+            .probe_timeout
+            .as_ref()
+            .or(m.probe_interval.as_ref())
+            .and_then(|l| l.loc());
+        ctx.error(
+            "V-122",
+            &at.key("probe_timeout"),
+            loc,
+            "probe_timeout must be below probe_interval",
+        );
+    }
+}
+
 pub(crate) fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
     if let Some(g) = &raw.globals {
         globals(g, ctx);
@@ -141,6 +206,9 @@ fn globals(g: &RawGlobals, ctx: &mut Ctx) {
                 "lock_backend kubernetes is reserved and not implemented in v1; use storage",
             );
         }
+        if let Some(m) = &c.memberlist {
+            gossip_timers(m, &at.key("memberlist"), ctx);
+        }
     }
     // V-097
     if let Some(t) = &g.tracing
```

Change `crates/data-avalanche-config/src/walk.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/walk.rs b/crates/data-avalanche-config/src/walk.rs
index 0419eaf..9a90474 100644
--- a/crates/data-avalanche-config/src/walk.rs
+++ b/crates/data-avalanche-config/src/walk.rs
@@ -275,24 +275,12 @@ walk_fields!(RawMemberlist {
     advertise_port,
     bind_addr,
     bind_port,
-    packet_dial_timeout,
-    packet_write_timeout,
-    compression_enabled,
     gossip_interval,
     gossip_nodes,
     probe_interval,
     probe_timeout,
-    dead_node_reclaim_time,
     push_pull_interval,
-    retransmit_factor,
-    lifeguard_enabled,
-    tls
-});
-walk_fields!(RawMemberlistTls {
-    enabled,
-    cert,
-    key,
-    ca
+    retransmit_factor
 });
 walk_fields!(RawTracing {
     otlp_endpoint,
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`

Expected: PASS, 149 tests.

```text
test result: ok. 83 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.07s
test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: ok. 45 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.05s
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.35s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1093 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/build.rs \
  crates/data-avalanche-config/src/diff.rs \
  crates/data-avalanche-config/src/mutability.rs \
  crates/data-avalanche-config/src/raw.rs \
  crates/data-avalanche-config/src/registry.rs \
  crates/data-avalanche-config/src/resolved.rs \
  crates/data-avalanche-config/src/rules/auth.rs \
  crates/data-avalanche-config/src/rules/domain.rs \
  crates/data-avalanche-config/src/walk.rs \
  crates/data-avalanche-config/tests/fixtures/spec_example.yaml \
  crates/data-avalanche-config/tests/load.rs \
  crates/data-avalanche-config/tests/rules/auth.rs \
  crates/data-avalanche-config/tests/rules/domain.rs \
  crates/data-avalanche-config/tests/support/mod.rs
git commit -m "feat(config): globals.cluster.memberlist for foca — memberlist's TCP, compression, Lifeguard and TLS keys removed (V-054 retired); V-122 gossip timers"
```

---

### Task 2: The placement ring (`data-avalanche-cluster`)

**Files:**
- Modify: `Cargo.lock`
- Modify: `Cargo.toml`
- Modify: `crates/data-avalanche-cluster/Cargo.toml`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`
- Create: `crates/data-avalanche-cluster/src/ring.rs`

**Interfaces:**
- Consumes: nothing new.
- Produces (`ring.rs`, re-exported):
  - `pub struct Ring` (`Debug`, `Clone`, `PartialEq`, `Eq`)
  - `Ring::new<'a>(nodes: impl IntoIterator<Item = &'a str>, tokens: u32) -> Ring`
  - `Ring::owner(&self, pipeline_id: &str) -> Option<&str>`
  - `Ring::nodes(&self) -> &[String]` (sorted, deduplicated); `Ring::is_empty(&self) -> bool`

D-M8a.6. Clustering §3.1 (B): the ring is a pure function of the members, so every node computes the same ring from the same members. Each node contributes `tokens` points, `xxh3_64(name ‖ index as big-endian u32)`. A pipeline belongs to the node of the first point at or after `xxh3_64(id)`, wrapping past the end. The points are sorted with the node's index in the sorted names as the tie-break, so neither member order nor duplicates change an owner. `the_placement_is_pinned` fixes the owners of a set of ids on three nodes at the default 128 tokens. A change of hash, encoding or tie order would move pipelines at an upgrade, and the test catches it. The workspace gains `xxhash-rust` (the anchor's `hash_algo: xxh3`).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index bc7d4f3..6d1ab0c 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -7,6 +7,7 @@ pub mod claim;
 pub mod controller;
 pub mod lease;
 pub mod reconciler;
+pub mod ring;
 #[cfg(test)]
 mod test_support;
 
```

Create `crates/data-avalanche-cluster/src/ring.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/ring.rs b/crates/data-avalanche-cluster/src/ring.rs
new file mode 100644
index 0000000..3e8a646
--- /dev/null
+++ b/crates/data-avalanche-cluster/src/ring.rs
@@ -0,0 +1,93 @@
+#[cfg(test)]
+mod tests {
+    use std::collections::BTreeMap;
+
+    use super::*;
+
+    fn ids(n: usize) -> Vec<String> {
+        (0..n).map(|i| format!("pipeline-{i}")).collect()
+    }
+
+    fn owners(ring: &Ring, ids: &[String]) -> Vec<String> {
+        ids.iter()
+            .map(|id| ring.owner(id).expect("an owner").to_owned())
+            .collect()
+    }
+
+    /// Every node of a cluster, and every version of this build, must place pipelines alike:
+    /// these owners are pinned. A change to the hash input fails here, not in a split cluster.
+    #[test]
+    fn the_placement_is_pinned() {
+        let ring = Ring::new(["worker-0", "worker-1", "worker-2"], 128);
+        let got: Vec<(&str, &str)> = ["orders", "events", "customers", "payments", "audit"]
+            .into_iter()
+            .map(|id| (id, ring.owner(id).unwrap()))
+            .collect();
+        assert_eq!(
+            got,
+            [
+                ("orders", "worker-1"),
+                ("events", "worker-0"),
+                ("customers", "worker-2"),
+                ("payments", "worker-1"),
+                ("audit", "worker-0"),
+            ]
+        );
+        assert_eq!(point("worker-0", 0), 0xdb10_e65a_1e40_fe5f);
+    }
+
+    #[test]
+    fn the_member_order_and_duplicates_dont_matter() {
+        let a = Ring::new(["c", "a", "b"], 64);
+        let b = Ring::new(["a", "b", "c", "a"], 64);
+        assert_eq!(a, b);
+        assert_eq!(a.nodes(), ["a", "b", "c"]);
+    }
+
+    #[test]
+    fn an_empty_ring_has_no_owner_and_one_node_owns_everything() {
+        assert_eq!(Ring::new([], 128).owner("orders"), None);
+        assert!(Ring::new([], 128).is_empty());
+        let one = Ring::new(["solo"], 128);
+        assert!(owners(&one, &ids(200)).iter().all(|o| o == "solo"));
+    }
+
+    /// 128 tokens a node keep three nodes within a few percent of a third each.
+    #[test]
+    fn the_pipelines_spread_evenly() {
+        let ring = Ring::new(["worker-0", "worker-1", "worker-2"], 128);
+        let ids = ids(6000);
+        let mut count: BTreeMap<String, usize> = BTreeMap::new();
+        for o in owners(&ring, &ids) {
+            *count.entry(o).or_default() += 1;
+        }
+        for (node, n) in &count {
+            let share = *n as f64 / ids.len() as f64;
+            assert!((0.25..0.42).contains(&share), "{node}: {share}");
+        }
+    }
+
+    /// A joining node takes about a quarter of the pipelines and nothing moves between the
+    /// others; a leaving node's pipelines are the only ones that move.
+    #[test]
+    fn a_join_or_a_leave_moves_only_what_it_must() {
+        let ids = ids(4000);
+        let three = owners(&Ring::new(["a", "b", "c"], 128), &ids);
+        let four = owners(&Ring::new(["a", "b", "c", "d"], 128), &ids);
+        let mut moved = 0;
+        for (before, after) in three.iter().zip(&four) {
+            if before != after {
+                assert_eq!(after, "d", "a move between the old nodes");
+                moved += 1;
+            }
+        }
+        let share = moved as f64 / ids.len() as f64;
+        assert!((0.15..0.35).contains(&share), "moved {share}");
+        let two = owners(&Ring::new(["a", "c"], 128), &ids);
+        for (before, after) in three.iter().zip(&two) {
+            if before != "b" {
+                assert_eq!(before, after, "a pipeline not on b moved");
+            }
+        }
+    }
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib ring::`

Expected: FAIL to compile: `Ring` and `point` don't exist yet.

```text
error[E0425]: cannot find type `Ring` in this scope  --> crates/data-avalanche-cluster/src/ring.rs:11:22
error[E0433]: cannot find type `Ring` in this scope  --> crates/data-avalanche-cluster/src/ring.rs:21:20
error[E0425]: cannot find function `point` in this scope  --> crates/data-avalanche-cluster/src/ring.rs:36:20
error[E0433]: cannot find type `Ring` in this scope  --> crates/data-avalanche-cluster/src/ring.rs:41:17
error[E0433]: cannot find type `Ring` in this scope  --> crates/data-avalanche-cluster/src/ring.rs:42:17
error[E0433]: cannot find type `Ring` in this scope  --> crates/data-avalanche-cluster/src/ring.rs:49:20
error[E0433]: cannot find type `Ring` in this scope  --> crates/data-avalanche-cluster/src/ring.rs:50:17
error[E0433]: cannot find type `Ring` in this scope  --> crates/data-avalanche-cluster/src/ring.rs:51:19
error[E0433]: cannot find type `Ring` in this scope  --> crates/data-avalanche-cluster/src/ring.rs:58:20
error[E0433]: cannot find type `Ring` in this scope  --> crates/data-avalanche-cluster/src/ring.rs:75:29
```

- [ ] **Step 3: Implement**

Change `Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index 9f13ec0..da63004 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1728,6 +1728,7 @@ dependencies = [
  "object_store 0.14.2",
  "tokio",
  "tracing",
+ "xxhash-rust",
 ]
 
 [[package]]
@@ -6707,6 +6708,12 @@ version = "0.13.6"
 source = "registry+https://github.com/rust-lang/crates.io-index"
 checksum = "66fee0b777b0f5ac1c69bb06d361268faafa61cd4682ae064a171c16c433e9e4"
 
+[[package]]
+name = "xxhash-rust"
+version = "0.8.19"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "550a2b930b62486a393c52d5c3b84bff264b28aa437ed64694d31e93b1757af7"
+
 [[package]]
 name = "yasna"
 version = "0.6.0"
```

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
index 32ef18c..556bf9d 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -79,6 +79,8 @@ prometheus-client = "=0.25.1"
 gethostname = "1.1"
 clap = { version = "4.6.7", features = ["derive"] }
 axum = { version = "0.8.9", default-features = false, features = ["http1", "tokio"] }
+# The cluster (M8a, roadmap §2.1): the ring's hash (the anchor's `hash_algo: xxh3`).
+xxhash-rust = { version = "0.8", features = ["xxh3"] }
 
 # Dependencies build without debug info: DataFusion's full debug build is over 6 GB, which fills
 # CI runners. Workspace crates keep it.
```

Change `crates/data-avalanche-cluster/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-cluster/Cargo.toml b/crates/data-avalanche-cluster/Cargo.toml
index d66421c..297a63f 100644
--- a/crates/data-avalanche-cluster/Cargo.toml
+++ b/crates/data-avalanche-cluster/Cargo.toml
@@ -21,6 +21,7 @@ gethostname.workspace = true
 getrandom.workspace = true
 tokio = { workspace = true, features = ["sync"] }
 tracing.workspace = true
+xxhash-rust.workspace = true
 
 [dev-dependencies]
 arrow-array.workspace = true
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 6d1ab0c..8f240a5 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -18,3 +18,4 @@ pub use controller::{
 };
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
 pub use reconciler::{ClusterReconciler, node_identity};
+pub use ring::Ring;
```

Change `crates/data-avalanche-cluster/src/ring.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/ring.rs b/crates/data-avalanche-cluster/src/ring.rs
index 3e8a646..f7535fe 100644
--- a/crates/data-avalanche-cluster/src/ring.rs
+++ b/crates/data-avalanche-cluster/src/ring.rs
@@ -1,3 +1,63 @@
+//! The placement ring (Clustering §3.1, §4): each node puts `tokens` points on a 64-bit circle,
+//! and a pipeline belongs to the node of the first point at or after the hash of its `id`
+//! (wrapping round). The ring is a pure function of the node names and `tokens`, so every node
+//! that sees the same members computes the same owners; the hash is xxh3, the anchor's
+//! `hash_algo`. A node's points don't depend on the other nodes, so a node that joins or leaves
+//! moves only the pipelines it takes or gives up (§4.4).
+
+use xxhash_rust::xxh3::xxh3_64;
+
+/// The ring over a set of node names.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct Ring {
+    /// Sorted by point, then by node name.
+    points: Vec<(u64, usize)>,
+    /// Sorted, without duplicates.
+    nodes: Vec<String>,
+}
+
+/// Node `node`'s `index`-th point: xxh3 of its name's bytes followed by the index, 4 bytes
+/// big-endian. The index has a fixed width, so no two (name, index) pairs share their bytes.
+fn point(node: &str, index: u32) -> u64 {
+    let mut bytes = Vec::with_capacity(node.len() + 4);
+    bytes.extend_from_slice(node.as_bytes());
+    bytes.extend_from_slice(&index.to_be_bytes());
+    xxh3_64(&bytes)
+}
+
+impl Ring {
+    /// The ring of `nodes` (in any order, duplicates ignored), `tokens` points each.
+    pub fn new<'a>(nodes: impl IntoIterator<Item = &'a str>, tokens: u32) -> Self {
+        let mut nodes: Vec<String> = nodes.into_iter().map(str::to_owned).collect();
+        nodes.sort();
+        nodes.dedup();
+        let mut points: Vec<(u64, usize)> = nodes
+            .iter()
+            .enumerate()
+            .flat_map(|(n, name)| (0..tokens).map(move |i| (point(name, i), n)))
+            .collect();
+        points.sort_unstable();
+        Self { points, nodes }
+    }
+
+    /// The node that owns `pipeline_id`; `None` on an empty ring.
+    pub fn owner(&self, pipeline_id: &str) -> Option<&str> {
+        let h = xxh3_64(pipeline_id.as_bytes());
+        let at = self.points.partition_point(|&(p, _)| p < h);
+        let (_, n) = self.points.get(at).or_else(|| self.points.first())?;
+        Some(&self.nodes[*n])
+    }
+
+    /// The node names, sorted.
+    pub fn nodes(&self) -> &[String] {
+        &self.nodes
+    }
+
+    pub fn is_empty(&self) -> bool {
+        self.nodes.is_empty()
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use std::collections::BTreeMap;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib ring::`

Expected: PASS, 5 tests.

```text
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 41 filtered out; finished in 0.01s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1098 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  Cargo.toml \
  crates/data-avalanche-cluster/Cargo.toml \
  crates/data-avalanche-cluster/src/lib.rs \
  crates/data-avalanche-cluster/src/ring.rs
git commit -m "feat(cluster): the placement ring — xxh3 points per node, owners pinned"
```

---

### Task 3: Gossip packets and the wire (`data-avalanche-cluster`)

**Files:**
- Modify: `Cargo.lock`
- Modify: `crates/data-avalanche-cluster/Cargo.toml`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`
- Modify (test): `crates/data-avalanche-cluster/src/test_support.rs`
- Create: `crates/data-avalanche-cluster/src/wire.rs`

**Interfaces:**
- Consumes: `blake3` (workspace), `bytes`, `tokio` `net`.
- Produces (`wire.rs`, re-exported `GossipKey`, `Udp`, `Wire`):
  - `pub const MAC_LEN: usize = 32; pub const MAX_PAYLOAD: usize = 1400; pub const INBOX: usize = 1024;`
  - `pub struct GossipKey([u8; 32])` (`Clone`; its `Debug` prints `GossipKey(..)`)
  - `GossipKey::derive(fingerprint_key: &[u8; 32]) -> GossipKey`
  - `GossipKey::seal(&self, payload: &[u8]) -> Bytes`; `GossipKey::open<'a>(&self, packet: &'a [u8]) -> Option<&'a [u8]>`
  - `pub trait Wire: Send + Sync + Debug + 'static { fn send(&self, to: SocketAddr, packet: Bytes); }`
  - `Udp::bind(addrs: &[SocketAddr]) -> io::Result<(Udp, mpsc::Receiver<Bytes>)>` (`ClusterError` from Task 11); `Udp::local_addrs(&self) -> Vec<SocketAddr>`; `impl Wire for Udp`
  - `test_support`: `Hub` (`Hub::new() -> Arc<Hub>`, `port(self: &Arc<Hub>, addr) -> (Port, mpsc::Receiver<Bytes>)`, `cut(&self, a: &[SocketAddr], b: &[SocketAddr])`, `heal(&self)`), `Port: Wire`, `at(n) -> SocketAddr` (`10.0.0.n:7946`)

D-M8a.3, D-M8a.5. Q-M8-2: the gossip is authenticated, not encrypted. The key is derived from the anchor's `fingerprint_key`, so every node of the cluster has it and no config carries a secret. A node on another `globals.state` holds another anchor, hence another key. `open` compares the MAC in constant time (`blake3::Hash`'s `PartialEq`). `Udp` gives each socket a writer task with a queue. Right after `bind`, `try_send_to` returned `WouldBlock` (measured on macOS), and a writer that drains its queue after the gossip ends still sends the leave's packets. A full queue or inbox drops the packet: gossip is lossy by design. `Udp`'s drop aborts the readers only. The `Hub` gives the gossip tests a network on the paused clock, with cuts for partitions.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 8f240a5..af127e2 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -10,6 +10,7 @@ pub mod reconciler;
 pub mod ring;
 #[cfg(test)]
 mod test_support;
+pub mod wire;
 
 pub use claim::{Claim, Claimed, release};
 pub use controller::{
```

Change `crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index e7821ce..af902fc 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -1,6 +1,8 @@
-//! A store whose writes fail on cue, and the lease fixtures the tests share.
+//! A store whose writes fail on cue, the lease fixtures the tests share, and an in-memory
+//! network for the gossip.
 
 use std::collections::VecDeque;
+use std::net::SocketAddr;
 use std::sync::{Arc, Mutex};
 use std::time::Duration;
 
@@ -437,3 +439,87 @@ pub(crate) async fn eventually(what: &str, cond: impl Fn() -> bool) {
     }
     panic!("timed out waiting until {what}");
 }
+
+/// An in-memory network for the gossip: an inbox per address, and blocked (from, to) pairs.
+#[derive(Debug, Default)]
+pub(crate) struct Hub {
+    inboxes: Mutex<std::collections::HashMap<SocketAddr, tokio::sync::mpsc::Sender<Bytes>>>,
+    blocked: Mutex<std::collections::HashSet<(SocketAddr, SocketAddr)>>,
+}
+
+/// A node's connection to the hub.
+#[derive(Debug)]
+pub(crate) struct Port {
+    hub: Arc<Hub>,
+    addr: SocketAddr,
+}
+
+impl Hub {
+    pub fn new() -> Arc<Self> {
+        Arc::new(Self::default())
+    }
+
+    /// Plugs `addr` in: what it sends goes through the returned port, what it receives to the
+    /// returned inbox.
+    pub fn port(self: &Arc<Self>, addr: SocketAddr) -> (Port, tokio::sync::mpsc::Receiver<Bytes>) {
+        let (tx, rx) = tokio::sync::mpsc::channel(crate::wire::INBOX);
+        self.inboxes.lock().unwrap().insert(addr, tx);
+        let port = Port {
+            hub: Arc::clone(self),
+            addr,
+        };
+        (port, rx)
+    }
+
+    /// Drops all traffic between `a` and `b`, both ways.
+    pub fn cut(&self, a: &[SocketAddr], b: &[SocketAddr]) {
+        let mut blocked = self.blocked.lock().unwrap();
+        for x in a {
+            for y in b {
+                blocked.insert((*x, *y));
+                blocked.insert((*y, *x));
+            }
+        }
+    }
+
+    pub fn heal(&self) {
+        self.blocked.lock().unwrap().clear();
+    }
+}
+
+impl crate::Wire for Port {
+    fn send(&self, to: SocketAddr, packet: Bytes) {
+        if self.hub.blocked.lock().unwrap().contains(&(self.addr, to)) {
+            return;
+        }
+        if let Some(tx) = self.hub.inboxes.lock().unwrap().get(&to) {
+            let _ = tx.try_send(packet);
+        }
+    }
+}
+
+/// `10.0.0.<n>:7946`, a node's address on the hub.
+pub(crate) fn at(n: u8) -> SocketAddr {
+    SocketAddr::from(([10, 0, 0, n], 7946))
+}
+
+mod hub_tests {
+    use super::*;
+    use crate::Wire;
+
+    #[tokio::test]
+    async fn the_hub_delivers_until_cut_and_after_a_heal() {
+        let hub = Hub::new();
+        let (a, _a_in) = hub.port(at(1));
+        let (_b, mut b_in) = hub.port(at(2));
+        a.send(at(2), Bytes::from_static(b"1"));
+        assert_eq!(b_in.recv().await.as_deref(), Some(&b"1"[..]));
+        hub.cut(&[at(1)], &[at(2)]);
+        a.send(at(2), Bytes::from_static(b"2"));
+        assert!(b_in.try_recv().is_err(), "a cut drops the packet");
+        hub.heal();
+        a.send(at(2), Bytes::from_static(b"3"));
+        assert_eq!(b_in.recv().await.as_deref(), Some(&b"3"[..]));
+        a.send(at(9), Bytes::from_static(b"4"));
+    }
+}
```

Create `crates/data-avalanche-cluster/src/wire.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/wire.rs b/crates/data-avalanche-cluster/src/wire.rs
new file mode 100644
index 0000000..b48c5df
--- /dev/null
+++ b/crates/data-avalanche-cluster/src/wire.rs
@@ -0,0 +1,45 @@
+#[cfg(test)]
+mod tests {
+    use std::time::Duration;
+
+    use super::*;
+
+    #[test]
+    fn a_sealed_packet_opens_and_a_changed_one_doesnt() {
+        let key = GossipKey::derive(&[7; 32]);
+        let packet = key.seal(b"ping");
+        assert_eq!(packet.len(), 4 + MAC_LEN);
+        assert_eq!(key.open(&packet), Some(&b"ping"[..]));
+        for at in [0, 3, 4, packet.len() - 1] {
+            let mut bad = packet.to_vec();
+            bad[at] ^= 1;
+            assert_eq!(key.open(&bad), None, "a flipped byte at {at}");
+        }
+        assert_eq!(key.open(&packet[..MAC_LEN - 1]), None, "shorter than a MAC");
+        assert_eq!(key.open(&key.seal(b"")), Some(&b""[..]), "an empty payload");
+    }
+
+    /// Another cluster's anchor gives another key: its packets don't open here.
+    #[test]
+    fn another_anchor_is_another_key() {
+        let ours = GossipKey::derive(&[7; 32]);
+        let theirs = GossipKey::derive(&[8; 32]);
+        assert_ne!(ours, theirs);
+        assert_eq!(ours.open(&theirs.seal(b"ping")), None);
+        assert_eq!(format!("{ours:?}"), "GossipKey(..)");
+    }
+
+    #[tokio::test]
+    async fn packets_cross_udp_on_loopback() {
+        let any = SocketAddr::from(([127, 0, 0, 1], 0));
+        let (a, _a_in) = Udp::bind(&[any]).await.unwrap();
+        let (b, mut b_in) = Udp::bind(&[any]).await.unwrap();
+        let to = b.local_addrs()[0];
+        assert_ne!(to.port(), 0);
+        a.send(to, Bytes::from_static(b"hello"));
+        let got = tokio::time::timeout(Duration::from_secs(5), b_in.recv())
+            .await
+            .expect("a packet within 5 s");
+        assert_eq!(got.as_deref(), Some(&b"hello"[..]));
+    }
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib -- wire:: test_support::`

Expected: FAIL to compile: the `wire` module has only its tests; `Wire`, `MAC_LEN`, `INBOX` don't exist.

```text
error[E0432]: unresolved import `crate::Wire`
error[E0425]: cannot find value `INBOX` in module `crate::wire`
error[E0405]: cannot find trait `Wire` in the crate root
error[E0425]: cannot find value `MAC_LEN` in this scope  --> crates/data-avalanche-cluster/src/wire.rs:11:38
error[E0425]: cannot find value `MAC_LEN` in this scope  --> crates/data-avalanche-cluster/src/wire.rs:18:39
error[E0433]: cannot find type `SocketAddr` in this scope  --> crates/data-avalanche-cluster/src/wire.rs:34:19
error[E0433]: cannot find type `Bytes` in this scope  --> crates/data-avalanche-cluster/src/wire.rs:39:20
error[E0433]: cannot find type `GossipKey` in this scope  --> crates/data-avalanche-cluster/src/wire.rs:25:20
error[E0433]: cannot find type `GossipKey` in this scope  --> crates/data-avalanche-cluster/src/wire.rs:26:22
error[E0433]: cannot find type `Udp` in this scope  --> crates/data-avalanche-cluster/src/wire.rs:35:26
```

- [ ] **Step 3: Implement**

Change `Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index da63004..3824cdd 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1714,6 +1714,7 @@ version = "0.1.0"
 dependencies = [
  "arrow-array",
  "async-trait",
+ "blake3",
  "bytes",
  "chrono",
  "data-avalanche-config",
```

Change `crates/data-avalanche-cluster/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-cluster/Cargo.toml b/crates/data-avalanche-cluster/Cargo.toml
index 297a63f..f850de8 100644
--- a/crates/data-avalanche-cluster/Cargo.toml
+++ b/crates/data-avalanche-cluster/Cargo.toml
@@ -10,6 +10,8 @@ description = "Pipeline ownership for data-avalanche: the lease and its renewer,
 workspace = true
 
 [dependencies]
+blake3.workspace = true
+bytes.workspace = true
 chrono.workspace = true
 data-avalanche-config.workspace = true
 data-avalanche-core.workspace = true
@@ -19,14 +21,13 @@ data-avalanche-storage.workspace = true
 data-avalanche-telemetry.workspace = true
 gethostname.workspace = true
 getrandom.workspace = true
-tokio = { workspace = true, features = ["sync"] }
+tokio = { workspace = true, features = ["net", "sync"] }
 tracing.workspace = true
 xxhash-rust.workspace = true
 
 [dev-dependencies]
 arrow-array.workspace = true
 async-trait.workspace = true
-bytes.workspace = true
 data-avalanche-testkit.workspace = true
 object_store.workspace = true
 tokio = { workspace = true, features = ["test-util"] }
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index af127e2..33ae37b 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -20,3 +20,4 @@ pub use controller::{
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
 pub use reconciler::{ClusterReconciler, node_identity};
 pub use ring::Ring;
+pub use wire::{GossipKey, Udp, Wire};
```

Change `crates/data-avalanche-cluster/src/wire.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/wire.rs b/crates/data-avalanche-cluster/src/wire.rs
index b48c5df..691fbeb 100644
--- a/crates/data-avalanche-cluster/src/wire.rs
+++ b/crates/data-avalanche-cluster/src/wire.rs
@@ -1,3 +1,150 @@
+//! Gossip packets (Q-M8-2). Each packet is foca's bytes followed by a keyed BLAKE3 MAC whose key
+//! is derived from the cluster anchor's fingerprint key: every node that can read `globals.state`
+//! has it, no config carries it, and a packet without a valid MAC is dropped. The packets aren't
+//! encrypted; they carry node names, addresses and phases. They travel over UDP; the in-process
+//! tests carry them over a hub instead.
+
+use std::fmt;
+use std::io;
+use std::net::SocketAddr;
+use std::sync::Arc;
+
+use bytes::{Bytes, BytesMut};
+use tokio::net::UdpSocket;
+use tokio::sync::mpsc;
+use tokio::task::JoinHandle;
+
+/// The MAC's length: a full BLAKE3 output.
+pub const MAC_LEN: usize = 32;
+
+/// The largest payload foca writes (`max_packet_size`): with the MAC, a packet fits a 1500-byte
+/// MTU under IPv6 and UDP headers.
+pub const MAX_PAYLOAD: usize = 1400;
+
+/// The BLAKE3 key-derivation context of the gossip MAC key.
+const CONTEXT: &str = "data-avalanche 2026-10-20 gossip packet MAC v1";
+
+/// The key gossip packets are authenticated with.
+#[derive(Clone, PartialEq, Eq)]
+pub struct GossipKey([u8; 32]);
+
+impl fmt::Debug for GossipKey {
+    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
+        f.write_str("GossipKey(..)")
+    }
+}
+
+impl GossipKey {
+    /// The key of the cluster whose anchor holds `fingerprint_key`.
+    pub fn derive(fingerprint_key: &[u8; 32]) -> Self {
+        Self(blake3::derive_key(CONTEXT, fingerprint_key))
+    }
+
+    /// `payload` with its MAC appended.
+    pub fn seal(&self, payload: &[u8]) -> Bytes {
+        let mut packet = BytesMut::with_capacity(payload.len() + MAC_LEN);
+        packet.extend_from_slice(payload);
+        packet.extend_from_slice(blake3::keyed_hash(&self.0, payload).as_bytes());
+        packet.freeze()
+    }
+
+    /// The payload of a packet whose MAC is valid; `None` otherwise. The comparison takes the
+    /// same time wherever the MACs differ (`blake3::Hash`'s equality).
+    pub fn open<'a>(&self, packet: &'a [u8]) -> Option<&'a [u8]> {
+        let at = packet.len().checked_sub(MAC_LEN)?;
+        let (payload, mac) = packet.split_at(at);
+        let mac: [u8; MAC_LEN] = mac.try_into().ok()?;
+        (blake3::keyed_hash(&self.0, payload) == blake3::Hash::from_bytes(mac)).then_some(payload)
+    }
+}
+
+/// Where the gossip sends its packets. A send never waits: a packet that can't go at once is
+/// dropped, as UDP drops it, and SWIM's retries cover the loss.
+pub trait Wire: Send + Sync + fmt::Debug + 'static {
+    fn send(&self, to: SocketAddr, packet: Bytes);
+}
+
+/// How many received packets may wait for the gossip; more are dropped, as a full socket buffer
+/// drops them.
+pub const INBOX: usize = 1024;
+
+/// The gossip's UDP sockets, one per bind address. A reader per socket puts every packet it
+/// receives in the inbox; a writer per socket sends the packets queued for it, and ends once the
+/// queue is closed and empty, so the packets of a leave still go out after the gossip has ended.
+/// A packet goes out through the first socket of the destination's address family.
+#[derive(Debug)]
+pub struct Udp {
+    sockets: Vec<(SocketAddr, mpsc::Sender<(SocketAddr, Bytes)>)>,
+    readers: Vec<JoinHandle<()>>,
+}
+
+impl Udp {
+    /// Binds `addrs`; the packets they receive go to the returned inbox.
+    ///
+    /// # Errors
+    /// An address can't be bound.
+    pub async fn bind(addrs: &[SocketAddr]) -> io::Result<(Self, mpsc::Receiver<Bytes>)> {
+        let (inbox, rx) = mpsc::channel(INBOX);
+        let (mut sockets, mut readers) = (Vec::new(), Vec::new());
+        for addr in addrs {
+            let socket = Arc::new(UdpSocket::bind(addr).await?);
+            let (out, mut queued) = mpsc::channel::<(SocketAddr, Bytes)>(INBOX);
+            sockets.push((socket.local_addr()?, out));
+            let s = Arc::clone(&socket);
+            tokio::spawn(async move {
+                while let Some((to, packet)) = queued.recv().await {
+                    if let Err(e) = s.send_to(&packet, to).await {
+                        tracing::debug!("gossip send to {to}: {e}");
+                    }
+                }
+            });
+            let inbox = inbox.clone();
+            readers.push(tokio::spawn(async move {
+                let mut buf = vec![0u8; MAX_PAYLOAD + MAC_LEN + 1];
+                loop {
+                    match socket.recv_from(&mut buf).await {
+                        // A full inbox drops the packet; a closed one ends the reader.
+                        Ok((n, _)) => {
+                            if let Err(mpsc::error::TrySendError::Closed(_)) =
+                                inbox.try_send(Bytes::copy_from_slice(&buf[..n]))
+                            {
+                                return;
+                            }
+                        }
+                        Err(e) => tracing::debug!("gossip socket: {e}"),
+                    }
+                }
+            }));
+        }
+        Ok((Self { sockets, readers }, rx))
+    }
+
+    /// The bound addresses (a bind to port 0 shows the port chosen).
+    pub fn local_addrs(&self) -> Vec<SocketAddr> {
+        self.sockets.iter().map(|(a, _)| *a).collect()
+    }
+}
+
+impl Drop for Udp {
+    fn drop(&mut self) {
+        for r in &self.readers {
+            r.abort();
+        }
+    }
+}
+
+impl Wire for Udp {
+    fn send(&self, to: SocketAddr, packet: Bytes) {
+        let socket = self
+            .sockets
+            .iter()
+            .find(|(a, _)| a.is_ipv4() == to.is_ipv4());
+        if let Some((_, out)) = socket {
+            let _ = out.try_send((to, packet));
+        }
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use std::time::Duration;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib -- wire:: test_support::`

Expected: PASS, 4 tests.

```text
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 46 filtered out; finished in 0.00s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1102 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  crates/data-avalanche-cluster/Cargo.toml \
  crates/data-avalanche-cluster/src/lib.rs \
  crates/data-avalanche-cluster/src/test_support.rs \
  crates/data-avalanche-cluster/src/wire.rs
git commit -m "feat(cluster): gossip packets — a keyed BLAKE3 MAC from the anchor's fingerprint key (Q-M8-2), the UDP wire, an in-memory hub for the tests"
```

---

### Task 4: The gossip — foca 2.0 in a tokio actor (`data-avalanche-cluster`)

**Files:**
- Modify: `Cargo.lock`
- Modify: `Cargo.toml`
- Modify: `crates/data-avalanche-cluster/Cargo.toml`
- Create: `crates/data-avalanche-cluster/src/gossip.rs`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`

**Interfaces:**
- Consumes: `GossipKey`, `Wire`, `MAX_PAYLOAD` (Task 3); `test_support::Hub` (Task 3); `data_avalanche_config::Memberlist` (Task 1).
- Produces (`gossip.rs`, re-exported):
  - `pub enum Phase { Joining, Active, Draining, Leaving }` (serde, `Copy`)
  - `pub struct NodeMeta { pub phase: Phase, pub config_generation: u64, pub build: String }`
  - `pub struct NodeIdentity { pub node_id: String, pub incarnation: u64, pub rev: u32, pub addr: SocketAddr, pub meta: NodeMeta }`; `impl foca::Identity for NodeIdentity` (`Addr = SocketAddr`)
  - `pub struct Member { pub addr: SocketAddr, pub meta: NodeMeta, pub suspect: bool }`
  - `pub struct Members { pub me: String, pub nodes: BTreeMap<String, Member> }`; `Members::peers(&self) -> usize`
  - `pub struct GossipSettings { probe_interval, probe_timeout, gossip_interval, gossip_nodes: u32, push_pull_interval, retransmit_factor: u32 }`; `GossipSettings::new(&Memberlist)`
  - `Gossip::start(me: NodeIdentity, key: GossipKey, settings: GossipSettings, wire: Arc<dyn Wire>, inbox: mpsc::Receiver<Bytes>) -> Gossip`
  - `Gossip::members(&self) -> watch::Receiver<Members>`; `announce(&self, seeds: Vec<SocketAddr>)`; `announcer(&self) -> Announcer`; `set_phase(&self, Phase)`; `async leave(self)`
  - `pub struct Announcer` (`Clone`): `announce(&self, seeds: Vec<SocketAddr>)`
  - `Gossip`'s drop aborts the actor.

D-M8a.1, D-M8a.2, D-M8a.4. Q-M8-1: foca is sans-IO, so the actor owns it and feeds it the inbox's packets (opened with the key), its timer events (a `tokio::time::sleep` per scheduled timer) and our commands (announce, phase, leave). It hands foca's packets to the wire, sealed, and publishes `Members` on every notification. foca keys members by address (probe finding 4), so the actor groups identities by name and takes each name's highest `(incarnation, rev)` that foca doesn't hold down. A pod restarted at a new IP then replaces its old identity at once, and the old one, lingering as Suspect, is hidden. A phase change is `change_identity` with the next `rev`, which peers see as a rename, not a departure (finding 5). foca's settings follow config §6.5.1 through the formulas of D-M8a.4; the actor recomputes them with `set_config` when the member count changes. The workspace gains `foca` `=2.0.0` and `rand` `0.10`: foca's random source is a `StdRng` seeded from the OS.

- [ ] **Step 1: Write the failing tests**

Create `crates/data-avalanche-cluster/src/gossip.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/gossip.rs b/crates/data-avalanche-cluster/src/gossip.rs
new file mode 100644
index 0000000..6fbc147
--- /dev/null
+++ b/crates/data-avalanche-cluster/src/gossip.rs
@@ -0,0 +1,296 @@
+#[cfg(test)]
+mod tests {
+    use super::*;
+    use crate::test_support::{Hub, at, eventually};
+
+    fn settings() -> GossipSettings {
+        GossipSettings {
+            probe_interval: Duration::from_secs(1),
+            probe_timeout: Duration::from_millis(500),
+            gossip_interval: Duration::from_millis(200),
+            gossip_nodes: 3,
+            push_pull_interval: Duration::from_secs(5),
+            retransmit_factor: 4,
+        }
+    }
+
+    fn identity(name: &str, incarnation: u64, addr: SocketAddr) -> NodeIdentity {
+        NodeIdentity {
+            node_id: name.into(),
+            incarnation,
+            rev: 0,
+            addr,
+            meta: NodeMeta {
+                phase: Phase::Active,
+                config_generation: 1,
+                build: "1.0.0".into(),
+            },
+        }
+    }
+
+    const KEY: [u8; 32] = [7; 32];
+
+    fn start(hub: &Arc<Hub>, name: &str, incarnation: u64, addr: SocketAddr) -> Gossip {
+        start_with(hub, name, incarnation, addr, &KEY)
+    }
+
+    fn start_with(
+        hub: &Arc<Hub>,
+        name: &str,
+        incarnation: u64,
+        addr: SocketAddr,
+        key: &[u8; 32],
+    ) -> Gossip {
+        let (port, inbox) = hub.port(addr);
+        let me = identity(name, incarnation, addr);
+        Gossip::start(
+            me,
+            GossipKey::derive(key),
+            settings(),
+            Arc::new(port),
+            inbox,
+        )
+    }
+
+    fn names(g: &Gossip) -> Vec<String> {
+        g.members().borrow().nodes.keys().cloned().collect()
+    }
+
+    async fn three(hub: &Arc<Hub>) -> (Gossip, Gossip, Gossip) {
+        let (a, b, c) = (
+            start(hub, "a", 1, at(1)),
+            start(hub, "b", 1, at(2)),
+            start(hub, "c", 1, at(3)),
+        );
+        b.announce(vec![at(1)]);
+        c.announce(vec![at(1)]);
+        eventually("a, b and c everywhere", || {
+            [&a, &b, &c].iter().all(|g| names(g) == ["a", "b", "c"])
+        })
+        .await;
+        (a, b, c)
+    }
+
+    #[tokio::test(start_paused = true)]
+    async fn three_nodes_join_through_one_seed() {
+        let hub = Hub::new();
+        let t = Instant::now();
+        let (a, _b, _c) = three(&hub).await;
+        assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
+        let m = a.members().borrow().clone();
+        assert_eq!(m.me, "a");
+        assert_eq!(m.peers(), 2);
+        assert_eq!(m.nodes["b"].addr, at(2));
+        assert_eq!(m.nodes["b"].meta.phase, Phase::Active);
+        assert!(!m.nodes["b"].suspect);
+    }
+
+    #[tokio::test(start_paused = true)]
+    async fn a_phase_change_reaches_every_peer() {
+        let hub = Hub::new();
+        let (a, b, c) = three(&hub).await;
+        b.set_phase(Phase::Draining);
+        let phase = |g: &Gossip| g.members().borrow().nodes.get("b").map(|m| m.meta.phase);
+        eventually("b draining on a and c", || {
+            phase(&a) == Some(Phase::Draining) && phase(&c) == Some(Phase::Draining)
+        })
+        .await;
+        assert_eq!(phase(&b), Some(Phase::Draining), "its own view");
+        assert_eq!(names(&a), ["a", "b", "c"], "a phase change is no departure");
+    }
+
+    #[tokio::test(start_paused = true)]
+    async fn a_leave_is_seen_at_once() {
+        let hub = Hub::new();
+        let (a, b, c) = three(&hub).await;
+        let t = Instant::now();
+        b.leave().await;
+        eventually("b gone from a and c", || {
+            names(&a) == ["a", "c"] && names(&c) == ["a", "c"]
+        })
+        .await;
+        assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
+    }
+
+    /// A crash: the others suspect the node within a probe or two, and it stays a member until
+    /// the suspicion runs out (4 · max(1, log10 3) · 1 s = 4 s).
+    #[tokio::test(start_paused = true)]
+    async fn a_killed_node_is_suspect_then_gone() {
+        let hub = Hub::new();
+        let (a, b, c) = three(&hub).await;
+        let t = Instant::now();
+        drop(b);
+        let suspect = |g: &Gossip| {
+            g.members()
+                .borrow()
+                .nodes
+                .get("b")
+                .is_some_and(|m| m.suspect)
+        };
+        eventually("b suspect on a", || suspect(&a)).await;
+        let suspected = t.elapsed();
+        assert_eq!(names(&a), ["a", "b", "c"], "a suspect is still a member");
+        eventually("b gone from a and c", || {
+            names(&a) == ["a", "c"] && names(&c) == ["a", "c"]
+        })
+        .await;
+        let gone = t.elapsed();
+        assert!(
+            suspected < Duration::from_secs(4),
+            "suspected after {suspected:?}"
+        );
+        assert!(gone >= Duration::from_secs(4), "gone after {gone:?}");
+        assert!(gone < Duration::from_secs(12), "gone after {gone:?}");
+    }
+
+    /// A pod's restart: the same name, a later incarnation, another address. The new identity
+    /// counts at once; the old one lingers in foca until declared down, hidden by the grouping.
+    #[tokio::test(start_paused = true)]
+    async fn a_restart_at_a_new_address_counts_at_once() {
+        let hub = Hub::new();
+        let (a, b, c) = three(&hub).await;
+        drop(b);
+        tokio::time::sleep(Duration::from_secs(2)).await;
+        let b2 = start(&hub, "b", 2, at(4));
+        let t = Instant::now();
+        b2.announce(vec![at(1)]);
+        let addr = |g: &Gossip| {
+            g.members()
+                .borrow()
+                .nodes
+                .get("b")
+                .map(|m| (m.addr, m.suspect))
+        };
+        eventually("b at its new address on a and c", || {
+            addr(&a) == Some((at(4), false)) && addr(&c) == Some((at(4), false))
+        })
+        .await;
+        assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
+        tokio::time::sleep(Duration::from_secs(15)).await;
+        assert_eq!(
+            addr(&a),
+            Some((at(4), false)),
+            "the old identity's death changes nothing"
+        );
+    }
+
+    #[tokio::test(start_paused = true)]
+    async fn a_partition_splits_the_members_and_a_heal_joins_them() {
+        let hub = Hub::new();
+        let (a, b, c) = three(&hub).await;
+        hub.cut(&[at(1), at(2)], &[at(3)]);
+        eventually("{a, b} | {c}", || {
+            names(&a) == ["a", "b"] && names(&b) == ["a", "b"] && names(&c) == ["c"]
+        })
+        .await;
+        hub.heal();
+        let t = Instant::now();
+        eventually("healed", || {
+            [&a, &b, &c].iter().all(|g| names(g) == ["a", "b", "c"])
+        })
+        .await;
+        // An announce to the down members every push_pull_interval (5 s here).
+        assert!(
+            t.elapsed() < Duration::from_secs(11),
+            "healed after {:?}",
+            t.elapsed()
+        );
+    }
+
+    /// A node on another anchor has another key: its packets don't open, so it never joins.
+    #[tokio::test(start_paused = true)]
+    async fn a_node_with_another_key_cant_join() {
+        let hub = Hub::new();
+        let (a, _b, _c) = three(&hub).await;
+        let d = start_with(&hub, "d", 1, at(4), &[8; 32]);
+        d.announce(vec![at(1)]);
+        tokio::time::sleep(Duration::from_secs(30)).await;
+        assert_eq!(names(&a), ["a", "b", "c"]);
+        assert_eq!(names(&d), ["d"]);
+    }
+
+    /// The real wire: two nodes on loopback.
+    #[tokio::test]
+    async fn two_nodes_join_over_udp() {
+        async fn node(name: &str) -> Gossip {
+            let any = SocketAddr::from(([127, 0, 0, 1], 0));
+            let (udp, inbox) = crate::Udp::bind(&[any]).await.unwrap();
+            let me = identity(name, 1, udp.local_addrs()[0]);
+            Gossip::start(
+                me,
+                GossipKey::derive(&KEY),
+                settings(),
+                Arc::new(udp),
+                inbox,
+            )
+        }
+        let (a, b) = (node("a").await, node("b").await);
+        b.announce(vec![a.members().borrow().nodes["a"].addr]);
+        for g in [&a, &b] {
+            let mut m = g.members();
+            tokio::time::timeout(Duration::from_secs(10), m.wait_for(|m| m.peers() == 1))
+                .await
+                .expect("a and b see each other within 10 s")
+                .unwrap();
+        }
+        assert_eq!(names(&a), ["a", "b"]);
+        assert_eq!(names(&b), ["a", "b"]);
+    }
+
+    #[test]
+    fn the_members_are_each_names_latest_live_identity() {
+        let me = identity("a", 1, at(1));
+        let old = identity("b", 1, at(2));
+        let new = NodeIdentity {
+            rev: 3,
+            ..identity("b", 2, at(4))
+        };
+        let gone = identity("c", 1, at(3));
+        let state = [
+            (&me, State::Alive),
+            (&old, State::Suspect),
+            (&new, State::Alive),
+            (&gone, State::Down),
+        ];
+        let m = members_of(&me, state.into_iter());
+        assert_eq!(m.nodes.keys().collect::<Vec<_>>(), ["a", "b"]);
+        assert_eq!(m.nodes["b"].addr, at(4));
+        // A newer identity that is down doesn't hide an older live one (a clock that went back).
+        let down_new = [(&new, State::Down), (&old, State::Suspect)];
+        let m = members_of(&me, down_new.into_iter());
+        assert_eq!((m.nodes["b"].addr, m.nodes["b"].suspect), (at(2), true));
+    }
+
+    /// Config §6.5.1: `retransmit_factor · ceil(log10(n + 1))` transmissions and a suspicion of
+    /// `4 · max(1, log10 n) · probe_interval`.
+    #[test]
+    fn foca_follows_the_spec_formulas() {
+        let s = settings();
+        let at_size = |n| {
+            let c = foca_config(&s, n);
+            (c.max_transmissions.get(), c.suspect_to_down_after)
+        };
+        assert_eq!(at_size(1), (4, Duration::from_secs(4)));
+        assert_eq!(at_size(3), (4, Duration::from_secs(4)));
+        assert_eq!(at_size(10), (8, Duration::from_secs(4)));
+        assert_eq!(at_size(100), (12, Duration::from_secs(8)));
+        let c = foca_config(&s, 3);
+        assert_eq!(
+            (c.probe_period, c.probe_rtt),
+            (s.probe_interval, s.probe_timeout)
+        );
+        let p = |p: &Option<PeriodicParams>| p.as_ref().map(|p| (p.frequency, p.num_members.get()));
+        assert_eq!(p(&c.periodic_gossip), Some((Duration::from_millis(200), 3)));
+        assert_eq!(p(&c.periodic_announce), Some((Duration::from_secs(5), 1)));
+        assert_eq!(
+            p(&c.periodic_announce_to_down_members),
+            Some((Duration::from_secs(5), 2))
+        );
+        assert_eq!(c.max_packet_size.get(), MAX_PAYLOAD);
+        // Every size's settings replace any other's (foca refuses some changes).
+        let mut swim: Swim = Foca::new(identity("a", 1, at(1)), c, rand::make_rng(), PostcardCodec);
+        for n in [1, 2, 50, 1000] {
+            swim.set_config(foca_config(&s, n)).unwrap();
+        }
+    }
+}
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 33ae37b..56b5e21 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -5,6 +5,7 @@
 
 pub mod claim;
 pub mod controller;
+pub mod gossip;
 pub mod lease;
 pub mod reconciler;
 pub mod ring;
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib gossip::`

Expected: FAIL to compile: the `gossip` module has only its tests.

```text
error[E0425]: cannot find type `GossipSettings` in this scope
error[E0422]: cannot find struct, variant or union type `GossipSettings` in this scope
error[E0433]: cannot find type `Duration` in this scope  --> crates/data-avalanche-cluster/src/gossip.rs:10:30
error[E0433]: cannot find type `Duration` in this scope  --> crates/data-avalanche-cluster/src/gossip.rs:12:33
error[E0425]: cannot find type `SocketAddr` in this scope  --> crates/data-avalanche-cluster/src/gossip.rs:17:53
error[E0425]: cannot find type `NodeIdentity` in this scope  --> crates/data-avalanche-cluster/src/gossip.rs:17:68
error[E0422]: cannot find struct, variant or union type `NodeIdentity` in this scope  --> crates/data-avalanche-cluster/src/gossip.rs:18:9
error[E0422]: cannot find struct, variant or union type `NodeMeta` in this scope  --> crates/data-avalanche-cluster/src/gossip.rs:23:19
error[E0425]: cannot find type `Arc` in this scope  --> crates/data-avalanche-cluster/src/gossip.rs:33:20
error[E0425]: cannot find type `SocketAddr` in this scope  --> crates/data-avalanche-cluster/src/gossip.rs:33:66
```

- [ ] **Step 3: Implement**

Change `Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index 3824cdd..7b86326 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1326,6 +1326,15 @@ version = "0.5.4"
 source = "registry+https://github.com/rust-lang/crates.io-index"
 checksum = "0c9ea0ac24bc397ab3c98583a3c9ba74fa56b09a4449bbe172b9b1ddb016027a"
 
+[[package]]
+name = "cobs"
+version = "0.3.0"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "0fa961b519f0b462e3a3b4a34b64d119eeaca1d59af726fe450bbba07a9fc0a1"
+dependencies = [
+ "thiserror 2.0.21",
+]
+
 [[package]]
 name = "colorchoice"
 version = "1.0.5"
@@ -1724,9 +1733,12 @@ dependencies = [
  "data-avalanche-storage",
  "data-avalanche-telemetry",
  "data-avalanche-testkit",
+ "foca",
  "gethostname",
  "getrandom 0.3.4",
  "object_store 0.14.2",
+ "rand 0.10.3",
+ "serde",
  "tokio",
  "tracing",
  "xxhash-rust",
@@ -3025,6 +3037,18 @@ version = "1.0.7"
 source = "registry+https://github.com/rust-lang/crates.io-index"
 checksum = "3f9eec918d3f24069decb9af1554cad7c880e2da24a9afd88aca000531ab82c1"
 
+[[package]]
+name = "foca"
+version = "2.0.0"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "cfdf03bd44a0044a679493054cf0a78696bcb8dc4140dadaa350af0a04b3bbe1"
+dependencies = [
+ "bytes",
+ "postcard",
+ "rand 0.10.3",
+ "serde",
+]
+
 [[package]]
 name = "foldhash"
 version = "0.1.5"
@@ -4486,6 +4510,16 @@ version = "0.3.34"
 source = "registry+https://github.com/rust-lang/crates.io-index"
 checksum = "f6b464fbc74e149a392436b17d523f769e057cb6877f6a5c4618bc6f11800548"
 
+[[package]]
+name = "postcard"
+version = "1.1.3"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "6764c3b5dd454e283a30e6dfe78e9b31096d9e32036b5d1eaac7a6119ccb9a24"
+dependencies = [
+ "cobs",
+ "serde",
+]
+
 [[package]]
 name = "postgres-protocol"
 version = "0.6.12"
```

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
index 556bf9d..b21fe5b 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -79,8 +79,11 @@ prometheus-client = "=0.25.1"
 gethostname = "1.1"
 clap = { version = "4.6.7", features = ["derive"] }
 axum = { version = "0.8.9", default-features = false, features = ["http1", "tokio"] }
-# The cluster (M8a, roadmap §2.1): the ring's hash (the anchor's `hash_algo: xxh3`).
+# The cluster (M8a, roadmap §2.1): the ring's hash (the anchor's `hash_algo: xxh3`); the SWIM
+# core the gossip drives (Q-M8-1, probe `prototypes/cluster-capability`) and its random source.
 xxhash-rust = { version = "0.8", features = ["xxh3"] }
+foca = { version = "=2.0.0", default-features = false, features = ["std", "serde", "postcard-codec"] }
+rand = { version = "0.10", default-features = false, features = ["std", "std_rng", "sys_rng"] }
 
 # Dependencies build without debug info: DataFusion's full debug build is over 6 GB, which fills
 # CI runners. Workspace crates keep it.
```

Change `crates/data-avalanche-cluster/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-cluster/Cargo.toml b/crates/data-avalanche-cluster/Cargo.toml
index f850de8..2aa8055 100644
--- a/crates/data-avalanche-cluster/Cargo.toml
+++ b/crates/data-avalanche-cluster/Cargo.toml
@@ -19,8 +19,11 @@ data-avalanche-pipeline.workspace = true
 data-avalanche-state.workspace = true
 data-avalanche-storage.workspace = true
 data-avalanche-telemetry.workspace = true
+foca.workspace = true
 gethostname.workspace = true
 getrandom.workspace = true
+rand.workspace = true
+serde.workspace = true
 tokio = { workspace = true, features = ["net", "sync"] }
 tracing.workspace = true
 xxhash-rust.workspace = true
```

Change `crates/data-avalanche-cluster/src/gossip.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/gossip.rs b/crates/data-avalanche-cluster/src/gossip.rs
index 6fbc147..18fca90 100644
--- a/crates/data-avalanche-cluster/src/gossip.rs
+++ b/crates/data-avalanche-cluster/src/gossip.rs
@@ -1,3 +1,433 @@
+//! The membership plane (Clustering §2–§3, Q-M8-1): SWIM with infection-style dissemination and
+//! suspicion, from foca 2.0, driven by one tokio task per node. The task owns the `Foca` state
+//! and feeds it the packets the wire receives, the timers it asked for and the node's commands;
+//! what foca sends goes out sealed (`GossipKey`), what arrives unsealed is dropped.
+//!
+//! foca knows a member by its address (probe finding 4), so a node is an *identity*: its name,
+//! the incarnation of its process, a revision bumped on each metadata change and on each rejoin,
+//! its address and its metadata. [`Members`] groups the identities by name: a node's current one
+//! is its latest live identity, and a node is a member while that identity is alive or suspect.
+
+use std::collections::BTreeMap;
+use std::net::SocketAddr;
+use std::num::{NonZeroU8, NonZeroU32, NonZeroUsize};
+use std::sync::Arc;
+use std::time::Duration;
+
+use bytes::Bytes;
+use foca::{AccumulatingRuntime, Foca, NoCustomBroadcast, PeriodicParams, PostcardCodec, State};
+use rand::rngs::StdRng;
+use serde::{Deserialize, Serialize};
+use tokio::sync::{mpsc, oneshot, watch};
+use tokio::time::Instant;
+
+use crate::wire::{GossipKey, MAX_PAYLOAD, Wire};
+
+/// Where a node is in its life (Clustering §3.4). Only an `Active` node is placed on the ring.
+#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
+pub enum Phase {
+    /// Started; not yet given pipelines.
+    Joining,
+    Active,
+    /// Shutting down: handing its pipelines off.
+    Draining,
+    /// Its pipelines are released; about to leave the gossip.
+    Leaving,
+}
+
+/// What a node tells the others about itself.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+pub struct NodeMeta {
+    pub phase: Phase,
+    /// The config generation the node runs (`globals.state`, §12).
+    pub config_generation: u64,
+    /// The build, e.g. `1.0.0`.
+    pub build: String,
+}
+
+/// One incarnation of a node, as foca knows it.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+pub struct NodeIdentity {
+    pub node_id: String,
+    /// The process's start, in Unix milliseconds: a restarted node's identity is newer.
+    pub incarnation: u64,
+    /// Bumped on every metadata change and every rejoin.
+    pub rev: u32,
+    /// The advertised address: the others send here.
+    pub addr: SocketAddr,
+    pub meta: NodeMeta,
+}
+
+impl NodeIdentity {
+    fn version(&self) -> (u64, u32) {
+        (self.incarnation, self.rev)
+    }
+}
+
+impl foca::Identity for NodeIdentity {
+    type Addr = SocketAddr;
+
+    /// A node declared down while it runs (a partition, a pause) rejoins at once.
+    fn renew(&self) -> Option<Self> {
+        Some(Self {
+            rev: self.rev.wrapping_add(1),
+            ..self.clone()
+        })
+    }
+
+    fn addr(&self) -> SocketAddr {
+        self.addr
+    }
+
+    fn win_addr_conflict(&self, other: &Self) -> bool {
+        self.version() > other.version()
+    }
+}
+
+/// A node as the others see it.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct Member {
+    pub addr: SocketAddr,
+    pub meta: NodeMeta,
+    /// Suspected of having failed; still a member (D-11: suspects stay on the ring).
+    pub suspect: bool,
+}
+
+/// The nodes one node counts as members, itself included.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct Members {
+    pub me: String,
+    pub nodes: BTreeMap<String, Member>,
+}
+
+impl Members {
+    /// The other members.
+    pub fn peers(&self) -> usize {
+        self.nodes.len().saturating_sub(1)
+    }
+}
+
+/// The members `me` sees in foca's membership state: each name's latest identity that isn't
+/// down (a restarted node's previous identity lingers as suspect until it is declared down), and
+/// no name whose identities are all down.
+fn members_of<'a>(
+    me: &NodeIdentity,
+    state: impl Iterator<Item = (&'a NodeIdentity, State)>,
+) -> Members {
+    let mut latest: BTreeMap<&str, (&NodeIdentity, State)> = BTreeMap::new();
+    for (id, s) in state {
+        if id.node_id == me.node_id || s == State::Down {
+            continue;
+        }
+        let e = latest.entry(&id.node_id).or_insert((id, s));
+        if id.version() > e.0.version() {
+            *e = (id, s);
+        }
+    }
+    let mut nodes: BTreeMap<String, Member> = latest
+        .into_iter()
+        .map(|(name, (id, s))| {
+            let member = Member {
+                addr: id.addr,
+                meta: id.meta.clone(),
+                suspect: s == State::Suspect,
+            };
+            (name.to_owned(), member)
+        })
+        .collect();
+    let mine = Member {
+        addr: me.addr,
+        meta: me.meta.clone(),
+        suspect: false,
+    };
+    nodes.insert(me.node_id.clone(), mine);
+    Members {
+        me: me.node_id.clone(),
+        nodes,
+    }
+}
+
+/// The gossip's timers (config §6.5.1).
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct GossipSettings {
+    pub probe_interval: Duration,
+    pub probe_timeout: Duration,
+    pub gossip_interval: Duration,
+    pub gossip_nodes: u32,
+    pub push_pull_interval: Duration,
+    pub retransmit_factor: u32,
+}
+
+impl GossipSettings {
+    pub fn new(m: &data_avalanche_config::Memberlist) -> Self {
+        Self {
+            probe_interval: m.probe_interval,
+            probe_timeout: m.probe_timeout,
+            gossip_interval: m.gossip_interval,
+            gossip_nodes: m.gossip_nodes,
+            push_pull_interval: m.push_pull_interval,
+            retransmit_factor: m.retransmit_factor,
+        }
+    }
+}
+
+/// How long a suspect has to refute before it is declared down, as a multiple of
+/// `max(1, log10 n) · probe_interval` (memberlist's `SuspicionMult`).
+const SUSPICION_MULT: f64 = 4.0;
+
+/// How many members a probe asks to probe indirectly.
+const INDIRECT_PROBES: usize = 3;
+
+/// How long a down member is remembered, and announced to so that a healed partition rejoins
+/// (foca's default). A newer identity of the same node is accepted at once whatever this is.
+const REMEMBER_DOWN: Duration = Duration::from_secs(24 * 60 * 60);
+
+/// foca's settings for a cluster of `n` members. The settings that scale with the cluster are
+/// set again whenever its size changes; the others never change (foca refuses that).
+fn foca_config(s: &GossipSettings, n: usize) -> foca::Config {
+    let n = u32::try_from(n.max(1)).unwrap_or(u32::MAX);
+    let log = f64::from(n).log10();
+    let tx = s.retransmit_factor as f64 * f64::from(n.saturating_add(1)).log10().ceil();
+    let every = |frequency, num| PeriodicParams {
+        frequency,
+        num_members: NonZeroUsize::new(num).unwrap_or(NonZeroUsize::MIN),
+    };
+    let mut c = foca::Config::new_lan(NonZeroU32::MIN);
+    c.probe_period = s.probe_interval;
+    c.probe_rtt = s.probe_timeout;
+    c.num_indirect_probes = NonZeroUsize::new(INDIRECT_PROBES).unwrap_or(NonZeroUsize::MIN);
+    c.max_transmissions = NonZeroU8::new(tx.clamp(1.0, 255.0) as u8).unwrap_or(NonZeroU8::MIN);
+    c.suspect_to_down_after = s.probe_interval.mul_f64(SUSPICION_MULT * log.max(1.0));
+    c.remove_down_after = Some(REMEMBER_DOWN);
+    c.max_packet_size = NonZeroUsize::new(MAX_PAYLOAD).unwrap_or(NonZeroUsize::MIN);
+    c.notify_down_members = true;
+    c.periodic_gossip = Some(every(s.gossip_interval, s.gossip_nodes as usize));
+    c.periodic_announce = Some(every(s.push_pull_interval, 1));
+    c.periodic_announce_to_down_members = Some(every(s.push_pull_interval, 2));
+    c
+}
+
+/// How often at most a dropped packet is warned about.
+const WARN_EVERY: Duration = Duration::from_secs(60);
+
+enum Command {
+    Announce(Vec<SocketAddr>),
+    Phase(Phase),
+    Leave(oneshot::Sender<()>),
+}
+
+/// A running gossip. Dropping it stops the gossip without a leave, as a crash would.
+#[derive(Debug)]
+pub struct Gossip {
+    commands: mpsc::UnboundedSender<Command>,
+    members: watch::Receiver<Members>,
+    task: tokio::task::JoinHandle<()>,
+}
+
+impl Drop for Gossip {
+    fn drop(&mut self) {
+        self.task.abort();
+    }
+}
+
+type Swim = Foca<NodeIdentity, PostcardCodec, StdRng, NoCustomBroadcast>;
+
+impl Gossip {
+    /// Starts the gossip as `me`: it sends through `wire` and reads what `inbox` receives.
+    pub fn start(
+        me: NodeIdentity,
+        key: GossipKey,
+        settings: GossipSettings,
+        wire: Arc<dyn Wire>,
+        inbox: mpsc::Receiver<Bytes>,
+    ) -> Self {
+        let swim = Foca::new(
+            me.clone(),
+            foca_config(&settings, 1),
+            rand::make_rng(),
+            PostcardCodec,
+        );
+        let (members_tx, members) = watch::channel(members_of(&me, std::iter::empty()));
+        let (commands, rx) = mpsc::unbounded_channel();
+        let actor = Actor {
+            swim,
+            key,
+            settings,
+            wire,
+            members: members_tx,
+            size: 1,
+            dropped: 0,
+            warned: None,
+        };
+        let task = tokio::spawn(actor.run(inbox, rx));
+        Self {
+            commands,
+            members,
+            task,
+        }
+    }
+
+    /// The members, as they change.
+    pub fn members(&self) -> watch::Receiver<Members> {
+        self.members.clone()
+    }
+
+    /// Asks `seeds` to let this node in. A seed is known by its address only.
+    pub fn announce(&self, seeds: Vec<SocketAddr>) {
+        let _ = self.commands.send(Command::Announce(seeds));
+    }
+
+    /// Tells the others this node's new phase.
+    pub fn set_phase(&self, phase: Phase) {
+        let _ = self.commands.send(Command::Phase(phase));
+    }
+
+    /// Leaves the cluster: the others see this node gone at once, not after a suspicion.
+    pub async fn leave(self) {
+        let (tx, rx) = oneshot::channel();
+        if self.commands.send(Command::Leave(tx)).is_ok() {
+            let _ = rx.await;
+        }
+    }
+}
+
+struct Actor {
+    swim: Swim,
+    key: GossipKey,
+    settings: GossipSettings,
+    wire: Arc<dyn Wire>,
+    members: watch::Sender<Members>,
+    /// The cluster size foca's settings were last computed for.
+    size: usize,
+    dropped: u64,
+    warned: Option<Instant>,
+}
+
+impl Actor {
+    async fn run(
+        mut self,
+        mut inbox: mpsc::Receiver<Bytes>,
+        mut commands: mpsc::UnboundedReceiver<Command>,
+    ) {
+        let mut rt = AccumulatingRuntime::new();
+        let (timers_tx, mut timers) = mpsc::unbounded_channel();
+        loop {
+            let done = tokio::select! {
+                Some(packet) = inbox.recv() => {
+                    match self.key.open(&packet) {
+                        Some(payload) => log(self.swim.handle_data(payload, &mut rt)),
+                        None => self.dropped(),
+                    }
+                    None
+                }
+                Some(timer) = timers.recv() => {
+                    log(self.swim.handle_timer(timer, &mut rt));
+                    None
+                }
+                command = commands.recv() => match command {
+                    Some(Command::Announce(seeds)) => {
+                        for addr in seeds {
+                            log(self.swim.announce(seed(addr), &mut rt));
+                        }
+                        None
+                    }
+                    Some(Command::Phase(phase)) => {
+                        let me = self.swim.identity();
+                        let next = NodeIdentity {
+                            rev: me.rev.wrapping_add(1),
+                            meta: NodeMeta { phase, ..me.meta.clone() },
+                            ..me.clone()
+                        };
+                        log(self.swim.change_identity(next, &mut rt));
+                        None
+                    }
+                    Some(Command::Leave(done)) => {
+                        log(self.swim.leave_cluster(&mut rt));
+                        Some(Some(done))
+                    }
+                    // The handle is gone.
+                    None => Some(None),
+                },
+            };
+            while let Some((to, payload)) = rt.to_send() {
+                self.wire.send(to.addr, self.key.seal(&payload));
+            }
+            while let Some((after, timer)) = rt.to_schedule() {
+                let tx = timers_tx.clone();
+                tokio::spawn(async move {
+                    tokio::time::sleep(after).await;
+                    let _ = tx.send(timer);
+                });
+            }
+            while let Some(n) = rt.to_notify() {
+                tracing::debug!("gossip: {n:?}");
+            }
+            if let Some(done) = done {
+                if let Some(done) = done {
+                    let _ = done.send(());
+                }
+                return;
+            }
+            self.publish();
+        }
+    }
+
+    fn publish(&mut self) {
+        let me = self.swim.identity();
+        let next = members_of(
+            me,
+            self.swim
+                .iter_membership_state()
+                .map(|m| (m.id(), m.state())),
+        );
+        let size = next.nodes.len();
+        if size != self.size {
+            self.size = size;
+            log(self.swim.set_config(foca_config(&self.settings, size)));
+        }
+        self.members.send_if_modified(|m| {
+            let changed = *m != next;
+            *m = next;
+            changed
+        });
+    }
+
+    /// A packet that doesn't open: another cluster's, a corrupted one, or forged.
+    fn dropped(&mut self) {
+        self.dropped += 1;
+        if self.warned.is_none_or(|t| t.elapsed() >= WARN_EVERY) {
+            tracing::warn!(
+                dropped = self.dropped,
+                "gossip: dropped packets whose MAC doesn't match this cluster's key: another \
+                 cluster's nodes on this network, or a node of this cluster on another anchor"
+            );
+            self.warned = Some(Instant::now());
+        }
+    }
+}
+
+/// An announce's destination: foca needs an identity, but only its address is used.
+fn seed(addr: SocketAddr) -> NodeIdentity {
+    NodeIdentity {
+        node_id: String::new(),
+        incarnation: 0,
+        rev: 0,
+        addr,
+        meta: NodeMeta {
+            phase: Phase::Joining,
+            config_generation: 0,
+            build: String::new(),
+        },
+    }
+}
+
+/// foca's errors are about a packet or a call it refused; the protocol recovers from them.
+fn log(r: Result<(), foca::Error>) {
+    if let Err(e) = r {
+        tracing::debug!("gossip: {e}");
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use super::*;
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 56b5e21..3d2d755 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -18,6 +18,7 @@ pub use controller::{
     Actual, ControllerDeps, ControllerSettings, ControllerStatus, Parts, PipelineController,
     Target, WorkerFactory,
 };
+pub use gossip::{Gossip, GossipSettings, Member, Members, NodeIdentity, NodeMeta, Phase};
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
 pub use reconciler::{ClusterReconciler, node_identity};
 pub use ring::Ring;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib gossip::`

Expected: PASS, 10 tests.

```text
test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.03s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1112 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  Cargo.toml \
  crates/data-avalanche-cluster/Cargo.toml \
  crates/data-avalanche-cluster/src/gossip.rs \
  crates/data-avalanche-cluster/src/lib.rs
git commit -m "feat(cluster): the gossip — foca 2.0 driven by a tokio actor (Q-M8-1): node identities with phase and generation, members by name, foca's settings from config §6.5.1"
```

---

### Task 5: Placement — the ring over the Active members (`data-avalanche-cluster`)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/lib.rs`
- Create: `crates/data-avalanche-cluster/src/placement.rs`

**Interfaces:**
- Consumes: `Ring` (Task 2); `Members`, `Member`, `Phase` (Task 4).
- Produces (`placement.rs`, re-exported `Placement`):
  - `Placement::new(tokens: u32, stabilization: Duration) -> Placement`
  - `Placement::update(&mut self, members: &Members, now: Instant) -> bool` (true when the ring changed)
  - `Placement::next_due(&self) -> Option<Instant>` (the next addition's time)
  - `Placement::ring(&self) -> &Ring`; `Placement::generation(&self) -> u64` (bumped on each change)

D-M8a.7, Clustering §20.3 (D-11). Eligible means a member in phase `Active`; a suspect stays eligible. A node that stops being eligible leaves the ring at once. A newly eligible node joins after `rebalance_stabilization` of continuous eligibility, and losing eligibility in the meantime restarts its wait. `Draining` is off the ring at once: the owners-to-be claim while the draining node hands off (Task 7). The first ring of a fresh node takes every eligible peer at once, and the node itself only when no peer is eligible. A joiner then takes the cluster's ring as it is, and waits for its own place.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 3d2d755..aec06d9 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -7,6 +7,7 @@ pub mod claim;
 pub mod controller;
 pub mod gossip;
 pub mod lease;
+pub mod placement;
 pub mod reconciler;
 pub mod ring;
 #[cfg(test)]
```

Create `crates/data-avalanche-cluster/src/placement.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/placement.rs b/crates/data-avalanche-cluster/src/placement.rs
new file mode 100644
index 0000000..8d605aa
--- /dev/null
+++ b/crates/data-avalanche-cluster/src/placement.rs
@@ -0,0 +1,133 @@
+#[cfg(test)]
+mod tests {
+    use super::*;
+    use crate::test_support::at;
+    use crate::{Member, NodeMeta};
+
+    const STABLE: Duration = Duration::from_secs(15);
+
+    /// `me`'s view: each `(name, phase, suspect)`.
+    fn members(me: &str, nodes: &[(&str, Phase, bool)]) -> Members {
+        let nodes = nodes
+            .iter()
+            .enumerate()
+            .map(|(i, (name, phase, suspect))| {
+                let member = Member {
+                    addr: at(u8::try_from(i).unwrap() + 1),
+                    meta: NodeMeta {
+                        phase: *phase,
+                        config_generation: 1,
+                        build: "1.0.0".into(),
+                    },
+                    suspect: *suspect,
+                };
+                ((*name).to_owned(), member)
+            })
+            .collect();
+        Members {
+            me: me.into(),
+            nodes,
+        }
+    }
+
+    use Phase::{Active, Draining, Joining};
+
+    fn nodes(p: &Placement) -> Vec<&str> {
+        p.ring().nodes().iter().map(String::as_str).collect()
+    }
+
+    #[test]
+    fn a_lone_node_is_placed_at_once() {
+        let mut p = Placement::new(128, STABLE);
+        let t = Instant::now();
+        assert!(
+            !p.update(&members("a", &[("a", Joining, false)]), t),
+            "a joining node"
+        );
+        assert!(p.ring().is_empty());
+        assert!(p.update(&members("a", &[("a", Active, false)]), t));
+        assert_eq!(nodes(&p), ["a"]);
+        assert_eq!(p.generation(), 1);
+        assert_eq!(p.next_due(), None);
+    }
+
+    /// A node that starts into a running cluster takes its peers as they are and waits for
+    /// itself, as they wait for it.
+    #[test]
+    fn a_new_node_places_its_peers_at_once_and_itself_later() {
+        let mut p = Placement::new(128, STABLE);
+        let t = Instant::now();
+        let view = members(
+            "c",
+            &[
+                ("a", Active, false),
+                ("b", Active, false),
+                ("c", Active, false),
+            ],
+        );
+        assert!(p.update(&view, t));
+        assert_eq!(nodes(&p), ["a", "b"]);
+        assert_eq!(p.next_due(), Some(t + STABLE));
+        assert!(!p.update(&view, t + STABLE - Duration::from_millis(1)));
+        assert!(p.update(&view, t + STABLE));
+        assert_eq!(nodes(&p), ["a", "b", "c"]);
+        assert_eq!(p.generation(), 2);
+    }
+
+    #[test]
+    fn a_joiner_waits_out_the_stabilization_and_a_flap_restarts_its_wait() {
+        let mut p = Placement::new(128, STABLE);
+        let t = Instant::now();
+        p.update(&members("a", &[("a", Active, false)]), t);
+        let with_c = members("a", &[("a", Active, false), ("c", Active, false)]);
+        assert!(!p.update(&with_c, t + Duration::from_secs(1)));
+        // c goes away at 10 s and is back at 12 s: its wait starts again.
+        p.update(
+            &members("a", &[("a", Active, false)]),
+            t + Duration::from_secs(10),
+        );
+        p.update(&with_c, t + Duration::from_secs(12));
+        assert!(
+            !p.update(&with_c, t + Duration::from_secs(16)),
+            "not 15 s after its join"
+        );
+        assert_eq!(p.next_due(), Some(t + Duration::from_secs(27)));
+        assert!(p.update(&with_c, t + Duration::from_secs(27)));
+        assert_eq!(nodes(&p), ["a", "c"]);
+    }
+
+    #[test]
+    fn a_suspect_stays_and_a_departure_or_a_drain_leaves_at_once() {
+        let mut p = Placement::new(128, STABLE);
+        let t = Instant::now();
+        let three = [
+            ("a", Active, false),
+            ("b", Active, false),
+            ("c", Active, false),
+        ];
+        p.update(&members("a", &three), t);
+        assert_eq!(nodes(&p), ["b", "c"], "the peers first");
+        p.update(&members("a", &three), t + STABLE);
+        assert_eq!(nodes(&p), ["a", "b", "c"]);
+        let later = t + STABLE + Duration::from_secs(1);
+        let b_suspect = [
+            ("a", Active, false),
+            ("b", Active, true),
+            ("c", Active, false),
+        ];
+        assert!(
+            !p.update(&members("a", &b_suspect), later),
+            "a suspect stays"
+        );
+        let b_gone = [("a", Active, false), ("c", Active, false)];
+        assert!(p.update(&members("a", &b_gone), later));
+        assert_eq!(nodes(&p), ["a", "c"]);
+        let c_draining = [("a", Active, false), ("c", Draining, false)];
+        assert!(p.update(&members("a", &c_draining), later));
+        assert_eq!(nodes(&p), ["a"]);
+        // A node that drains itself leaves its own ring: it owns nothing.
+        assert!(p.update(&members("a", &[("a", Draining, false)]), later));
+        assert!(p.ring().is_empty());
+        assert_eq!(p.ring().owner("orders"), None);
+    }
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib placement::`

Expected: FAIL to compile: the `placement` module has only its tests.

```text
error[E0432]: unresolved import `Phase`  --> crates/data-avalanche-cluster/src/placement.rs:33:9
error[E0425]: cannot find type `Duration` in this scope
error[E0433]: cannot find type `Duration` in this scope
error[E0425]: cannot find type `Phase` in this scope  --> crates/data-avalanche-cluster/src/placement.rs:10:42
error[E0425]: cannot find type `Members` in this scope  --> crates/data-avalanche-cluster/src/placement.rs:10:60
error[E0422]: cannot find struct, variant or union type `Members` in this scope  --> crates/data-avalanche-cluster/src/placement.rs:27:9
error[E0425]: cannot find type `Placement` in this scope  --> crates/data-avalanche-cluster/src/placement.rs:35:18
error[E0433]: cannot find type `Instant` in this scope  --> crates/data-avalanche-cluster/src/placement.rs:42:17
error[E0433]: cannot find type `Instant` in this scope  --> crates/data-avalanche-cluster/src/placement.rs:59:17  --> crates/data-avalanche-cluster/src/placement.rs:71:47
error[E0433]: cannot find type `Instant` in this scope  --> crates/data-avalanche-cluster/src/placement.rs:80:17  --> crates/data-avalanche-cluster/src/placement.rs:83:40  --> crates/data-avalanche-cluster/src/placement.rs:87:17  --> crates/data-avalanche-cluster/src/placement.rs:89:31  --> crates/data-avalanche-cluster/src/placement.rs:91:36  --> crates/data-avalanche-cluster/src/placement.rs:94:43  --> crates/data-avalanche-cluster/src/placement.rs:95:39
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index aec06d9..a26bd6f 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -21,6 +21,7 @@ pub use controller::{
 };
 pub use gossip::{Gossip, GossipSettings, Member, Members, NodeIdentity, NodeMeta, Phase};
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
+pub use placement::Placement;
 pub use reconciler::{ClusterReconciler, node_identity};
 pub use ring::Ring;
 pub use wire::{GossipKey, Udp, Wire};
```

Change `crates/data-avalanche-cluster/src/placement.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/placement.rs b/crates/data-avalanche-cluster/src/placement.rs
index 8d605aa..4d97cc9 100644
--- a/crates/data-avalanche-cluster/src/placement.rs
+++ b/crates/data-avalanche-cluster/src/placement.rs
@@ -1,3 +1,103 @@
+//! Which members are on the ring (Clustering §8.5, §20.3, D-11). A member is eligible while it
+//! is alive or suspect and its phase is `Active`: a suspect stays, so a blip moves nothing. A
+//! member that stops being eligible (down, left, or no longer `Active`) leaves the ring at once;
+//! one that becomes eligible (a join, a rejoin) is added only once it stayed eligible for
+//! `rebalance_stabilization`, so a flapping node never grabs and drops pipelines.
+//!
+//! An empty ring is filled at once: a node that starts, having joined the gossip, takes the
+//! eligible peers it finds as they are (their rings already hold them), and itself at once only
+//! when it has no eligible peer (a lone node, or the first of a cold start).
+
+use std::collections::BTreeMap;
+use std::time::Duration;
+
+use tokio::time::Instant;
+
+use crate::{Members, Phase, Ring};
+
+/// The ring a node places pipelines on, as membership changes.
+#[derive(Debug)]
+pub struct Placement {
+    tokens: u32,
+    stabilization: Duration,
+    /// The eligible members, each with the instant it became eligible.
+    eligible: BTreeMap<String, Instant>,
+    ring: Ring,
+    /// Bumped on every change of the ring's nodes.
+    generation: u64,
+}
+
+impl Placement {
+    pub fn new(tokens: u32, stabilization: Duration) -> Self {
+        Self {
+            tokens,
+            stabilization,
+            eligible: BTreeMap::new(),
+            ring: Ring::new([], tokens),
+            generation: 0,
+        }
+    }
+
+    /// Takes in `members` as of `now`. True when the ring's nodes changed.
+    pub fn update(&mut self, members: &Members, now: Instant) -> bool {
+        let eligible: Vec<&str> = members
+            .nodes
+            .iter()
+            .filter(|(_, m)| m.meta.phase == Phase::Active)
+            .map(|(name, _)| name.as_str())
+            .collect();
+        self.eligible
+            .retain(|name, _| eligible.contains(&name.as_str()));
+        for name in &eligible {
+            self.eligible.entry((*name).to_owned()).or_insert(now);
+        }
+        let mut next: Vec<&str> = if self.ring.is_empty() {
+            let peers: Vec<&str> = eligible
+                .iter()
+                .copied()
+                .filter(|n| *n != members.me)
+                .collect();
+            if peers.is_empty() { eligible } else { peers }
+        } else {
+            self.ring
+                .nodes()
+                .iter()
+                .map(String::as_str)
+                .filter(|n| self.eligible.contains_key(*n))
+                .collect()
+        };
+        for (name, since) in &self.eligible {
+            if now.duration_since(*since) >= self.stabilization && !next.contains(&name.as_str()) {
+                next.push(name);
+            }
+        }
+        next.sort_unstable();
+        if next == self.ring.nodes() {
+            return false;
+        }
+        self.ring = Ring::new(next, self.tokens);
+        self.generation += 1;
+        true
+    }
+
+    /// When the next waiting member is due on the ring: the next `update` worth making.
+    pub fn next_due(&self) -> Option<Instant> {
+        self.eligible
+            .iter()
+            .filter(|(name, _)| !self.ring.nodes().contains(name))
+            .map(|(_, since)| *since + self.stabilization)
+            .min()
+    }
+
+    pub fn ring(&self) -> &Ring {
+        &self.ring
+    }
+
+    pub fn generation(&self) -> u64 {
+        self.generation
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use super::*;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib placement::`

Expected: PASS, 4 tests.

```text
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 60 filtered out; finished in 0.00s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1116 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/lib.rs \
  crates/data-avalanche-cluster/src/placement.rs
git commit -m "feat(cluster): placement — active members on the ring, suspects kept, removals at once, additions after rebalance_stabilization (D-11)"
```

---

### Task 6: The reconciler on the ring (`data-avalanche-cluster`, `data-avalanche-telemetry`)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/controller.rs`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`
- Modify: `crates/data-avalanche-cluster/src/placement.rs`
- Modify: `crates/data-avalanche-cluster/src/reconciler.rs`
- Modify (test): `crates/data-avalanche-cluster/src/test_support.rs`
- Modify: `crates/data-avalanche-telemetry/src/lib.rs`
- Modify (test): `crates/data-avalanche-telemetry/tests/metrics.rs`
- Modify: `crates/data-avalanche/src/run.rs`

**Interfaces:**
- Consumes: `Placement` (Task 5); `Members` (Task 4); M6b's `PipelineController`, `ControllerDeps`, `Target`, `ControllerStatus`.
- Produces:
  - `ClusterReconciler::start(pipelines: Vec<(Pipeline, PipelineDocs)>, deps: &ControllerDeps, members: watch::Receiver<Members>, settings: ReconcilerSettings) -> ClusterReconciler` (M6b's took no members)
  - `pub struct ReconcilerSettings { pub tokens: u32, pub stabilization: Duration, pub resync: Duration }`; `ReconcilerSettings::new(&Cluster)` (`tokens`, `rebalance_stabilization`, `lease_renewal_interval`)
  - `ControllerDeps.handoffs: Arc<Semaphore>` (`max_concurrent_handoffs` permits)
  - `PipelineController::reporting(pipeline, docs, deps, status: watch::Sender<ControllerStatus>) -> PipelineController`
  - `ClusterReconciler`'s drop aborts the reconcile task and every controller (D-M8a.12)
  - `Metrics::set_ring_generation(&self, u64)`, the gauge `data_avalanche_ring_generation`
  - `test_support`: `members(me, names)`, `alone(me)`, `ByPipeline`, `STABLE` (15 s)

D-M8a.9…12. The reconcile task wakes on three triggers: a members change, the next stabilization due, and every `resync`. On each wake it updates the placement, sets each controller's target (`Own` exactly when the ring's owner is this node), and restarts any controller whose task ended unasked (DA-OPS-7). The targets are level-triggered: a controller sees the latest target, not every change. A running controller whose target turns `DontOwn` asks for a hand-off permit and drains while it holds it, so a rebalance stops at most `max_concurrent_handoffs` workers at once (Clustering §8.5). With no worker it releases at once. A closed target (shutdown) is uncapped. A dropped reconciler is a crash: everything aborts and the leases expire. M6b's Phase 0 test moves onto the ring: `q`'s lease is held by another node at first, and `p`'s preflight retries longer, so the order of the two starts is fixed on the paused clock. The binary's call is adapted mechanically (its self-only members); Task 11 wires the gossip.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-cluster/src/placement.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/placement.rs b/crates/data-avalanche-cluster/src/placement.rs
index 4d97cc9..9fd00eb 100644
--- a/crates/data-avalanche-cluster/src/placement.rs
+++ b/crates/data-avalanche-cluster/src/placement.rs
@@ -101,35 +101,10 @@ impl Placement {
 #[cfg(test)]
 mod tests {
     use super::*;
-    use crate::test_support::at;
-    use crate::{Member, NodeMeta};
+    use crate::test_support::members;
 
     const STABLE: Duration = Duration::from_secs(15);
 
-    /// `me`'s view: each `(name, phase, suspect)`.
-    fn members(me: &str, nodes: &[(&str, Phase, bool)]) -> Members {
-        let nodes = nodes
-            .iter()
-            .enumerate()
-            .map(|(i, (name, phase, suspect))| {
-                let member = Member {
-                    addr: at(u8::try_from(i).unwrap() + 1),
-                    meta: NodeMeta {
-                        phase: *phase,
-                        config_generation: 1,
-                        build: "1.0.0".into(),
-                    },
-                    suspect: *suspect,
-                };
-                ((*name).to_owned(), member)
-            })
-            .collect();
-        Members {
-            me: me.into(),
-            nodes,
-        }
-    }
-
     use Phase::{Active, Draining, Joining};
 
     fn nodes(p: &Placement) -> Vec<&str> {
```

Change `crates/data-avalanche-cluster/src/reconciler.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/reconciler.rs b/crates/data-avalanche-cluster/src/reconciler.rs
index a98aeb6..db9389c 100644
--- a/crates/data-avalanche-cluster/src/reconciler.rs
+++ b/crates/data-avalanche-cluster/src/reconciler.rs
@@ -106,16 +106,17 @@ impl ClusterReconciler {
 #[cfg(test)]
 mod tests {
     use std::sync::Arc;
-    use std::time::Duration;
+    use std::sync::atomic::{AtomicBool, Ordering};
 
     use data_avalanche_core::{SourceError, TableRef};
     use data_avalanche_testkit::{FakeTxn, Method};
 
     use super::*;
     use crate::test_support::{
-        ScriptedStore, TestFactory, config, deps, eventually, leases, pipeline_docs, row, until,
+        ScriptedStore, TestFactory, alone, config, deps, eventually, leases, members,
+        pipeline_docs, row, until,
     };
-    use crate::{Actual, Parts, WorkerFactory};
+    use crate::{Parts, Phase, Ring, WorkerFactory};
 
     /// One [`TestFactory`] per pipeline id.
     #[derive(Debug)]
@@ -131,24 +132,51 @@ mod tests {
         s.actual == Some(Actual::Running)
     }
 
-    /// Two pipelines `p` and `q` on one node; `starts` workers may be in Phase 0 at once.
+    fn standby(s: &ControllerStatus) -> bool {
+        matches!(s.actual, Some(Actual::Standby { .. }))
+    }
+
+    const STABLE: Duration = Duration::from_secs(15);
+
+    fn reconciler_settings() -> ReconcilerSettings {
+        ReconcilerSettings {
+            tokens: 128,
+            stabilization: STABLE,
+            resync: Duration::from_secs(2),
+        }
+    }
+
+    /// A factory per pipeline id of `ids`, and the deps that build from them.
+    fn factories(
+        store: &Arc<ScriptedStore>,
+        ids: &[&str],
+    ) -> (BTreeMap<String, TestFactory>, ControllerDeps) {
+        let fs: BTreeMap<String, TestFactory> = ids
+            .iter()
+            .map(|id| ((*id).to_owned(), TestFactory::new(&["a"])))
+            .collect();
+        let mut d = deps("n1", store, &fs[ids[0]]);
+        d.factory = Arc::new(ByPipeline(fs.clone()));
+        (fs, d)
+    }
+
+    fn pipelines(ids: &[&str]) -> Vec<(Pipeline, PipelineDocs)> {
+        let mut cfg = config(ids).pipelines;
+        ids.iter()
+            .map(|id| (cfg.remove(*id).unwrap(), pipeline_docs()))
+            .collect()
+    }
+
+    /// Two pipelines `p` and `q` on a lone node; `starts` workers may be in Phase 0 at once.
     fn two(
         store: &Arc<ScriptedStore>,
         starts: usize,
     ) -> (ClusterReconciler, TestFactory, TestFactory, ControllerDeps) {
-        let (fp, fq) = (TestFactory::new(&["a"]), TestFactory::new(&["a"]));
-        let mut d = deps("n1", store, &fp);
-        d.factory = Arc::new(ByPipeline(BTreeMap::from([
-            ("p".to_owned(), fp.clone()),
-            ("q".to_owned(), fq.clone()),
-        ])));
+        let (fs, mut d) = factories(store, &["p", "q"]);
         d.starts = Arc::new(tokio::sync::Semaphore::new(starts));
-        let pipelines = config(&["p", "q"])
-            .pipelines
-            .into_values()
-            .map(|p| (p, pipeline_docs()))
-            .collect();
-        (ClusterReconciler::start(pipelines, &d), fp, fq, d)
+        let (_, view) = watch::channel(alone("n1"));
+        let r = ClusterReconciler::start(pipelines(&["p", "q"]), &d, view, reconciler_settings());
+        (r, fs["p"].clone(), fs["q"].clone(), d)
     }
 
     #[test]
@@ -165,6 +193,20 @@ mod tests {
         assert_eq!(h.node_id, gethostname::gethostname().into_string().unwrap());
     }
 
+    #[test]
+    fn the_settings_come_from_the_cluster() {
+        let c = config(&["p"]).globals.cluster;
+        let s = ReconcilerSettings::new(&c);
+        assert_eq!(
+            (s.tokens, s.stabilization, s.resync),
+            (
+                c.tokens,
+                c.rebalance_stabilization,
+                c.lease_renewal_interval
+            )
+        );
+    }
+
     #[tokio::test(start_paused = true)]
     async fn every_pipeline_runs_here_and_a_shutdown_releases_every_lease() {
         let store = ScriptedStore::new();
@@ -178,16 +220,13 @@ mod tests {
             .commit(FakeTxn::new().insert(&TableRef::new("public", "a"), row(2)));
         eventually("both land", || fp.ids("a") == [1] && fq.ids("a") == [2]).await;
         let text = d.metrics.encode();
-        assert!(
-            text.lines()
-                .any(|l| l == "data_avalanche_owned_pipelines 2"),
-            "{text}"
-        );
-        assert!(
-            text.lines()
-                .any(|l| l == "data_avalanche_cluster_members 1"),
-            "{text}"
-        );
+        for line in [
+            "data_avalanche_owned_pipelines 2",
+            "data_avalanche_cluster_members 1",
+            "data_avalanche_ring_generation 1",
+        ] {
+            assert!(text.lines().any(|l| l == line), "{line}: {text}");
+        }
         r.shutdown().await;
         for id in ["p", "q"] {
             let lease = leases(&store).observe(id).await.unwrap().unwrap();
@@ -201,29 +240,52 @@ mod tests {
         );
     }
 
+    /// A reconciler dropped without a shutdown is a crash: nothing releases or renews.
+    #[tokio::test(start_paused = true)]
+    async fn a_reconciler_dropped_is_a_crash_and_its_leases_stay() {
+        let store = ScriptedStore::new();
+        let (r, _, _, _) = two(&store, 2);
+        for (_, mut s) in r.statuses() {
+            until(&mut s, "running", running).await;
+        }
+        drop(r);
+        tokio::time::sleep(Duration::from_secs(1)).await;
+        let held = || async {
+            let mut v = Vec::new();
+            for id in ["p", "q"] {
+                let lease = leases(&store).observe(id).await.unwrap().unwrap();
+                assert!(!lease.doc.released, "{id}");
+                v.push(lease.version);
+            }
+            v
+        };
+        let before = held().await;
+        tokio::time::sleep(Duration::from_secs(20)).await;
+        assert_eq!(held().await, before, "no renewal after the crash");
+    }
+
     /// `max_concurrent_takeovers` 1: while `p`'s Phase 0 retries its preflight, `q` builds no
     /// worker; it starts once `p` streams.
     #[tokio::test(start_paused = true)]
     async fn one_phase_0_at_a_time_holds_the_other_start() {
         let store = ScriptedStore::new();
-        let (fp, fq) = (TestFactory::new(&["a"]), TestFactory::new(&["a"]));
-        for _ in 0..3_000 {
+        let (fs, mut d) = factories(&store, &["p", "q"]);
+        let (fp, fq) = (fs["p"].clone(), fs["q"].clone());
+        // About 30 s of Phase 0 (a 5 ms backoff each).
+        for _ in 0..6_000 {
             fp.source
                 .fail(Method::Preflight, SourceError::Transient("slow".into()));
         }
-        let mut d = deps("n1", &store, &fp);
-        d.factory = Arc::new(ByPipeline(BTreeMap::from([
-            ("p".to_owned(), fp.clone()),
-            ("q".to_owned(), fq.clone()),
-        ])));
         d.starts = Arc::new(tokio::sync::Semaphore::new(1));
-        let mut cfg = config(&["p", "q"]).pipelines;
-        // `p` first: the controllers start in pipeline-id order, and `p`'s takes the permit.
-        let pipelines = vec![
-            (cfg.remove("p").unwrap(), pipeline_docs()),
-            (cfg.remove("q").unwrap(), pipeline_docs()),
-        ];
-        let r = ClusterReconciler::start(pipelines, &d);
+        // `q` is claimed later: another holder's lease, taken over after a lease_duration
+        // (15 s); `p`'s lease is free, claimed at once, and its Phase 0 takes the permit.
+        let n0 = crate::test_support::holder("n0");
+        leases(&store)
+            .acquire("q", &n0, Duration::from_secs(1), chrono::Utc::now())
+            .await
+            .unwrap();
+        let (_, view) = watch::channel(alone("n1"));
+        let r = ClusterReconciler::start(pipelines(&["p", "q"]), &d, view, reconciler_settings());
         let mut st = r.statuses();
         eventually("p is in Phase 0", || fp.builds() == 1).await;
         tokio::time::sleep(Duration::from_secs(1)).await;
@@ -232,10 +294,126 @@ mod tests {
             Some(Actual::Starting),
             "p still retries its preflight"
         );
+        until(st.get_mut("q").unwrap(), "q claimed", |s| {
+            s.owner_epoch.is_some()
+        })
+        .await;
         assert_eq!(fq.builds(), 0, "q waits for the permit");
         until(st.get_mut("p").unwrap(), "p running", running).await;
         until(st.get_mut("q").unwrap(), "q running", running).await;
         assert_eq!(fq.builds(), 1);
         r.shutdown().await;
     }
+
+    /// A node joins: after `rebalance_stabilization` the pipelines the ring gives it are
+    /// drained and released here, the others keep running.
+    #[tokio::test(start_paused = true)]
+    async fn a_joiner_gets_its_pipelines_after_the_stabilization() {
+        let ids = ["orders", "events", "customers", "users"];
+        let store = ScriptedStore::new();
+        let (_, d) = factories(&store, &ids);
+        let (tx, view) = watch::channel(alone("n1"));
+        let r = ClusterReconciler::start(pipelines(&ids), &d, view, reconciler_settings());
+        let mut st = r.statuses();
+        for s in st.values_mut() {
+            until(s, "running", running).await;
+        }
+        let two = Ring::new(["n1", "n2"], 128);
+        let moving: Vec<&str> = ids
+            .iter()
+            .copied()
+            .filter(|id| two.owner(id) == Some("n2"))
+            .collect();
+        assert!(!moving.is_empty() && moving.len() < ids.len(), "{moving:?}");
+        tx.send_replace(members(
+            "n1",
+            &[("n1", Phase::Active, false), ("n2", Phase::Active, false)],
+        ));
+        tokio::time::sleep(STABLE - Duration::from_secs(1)).await;
+        assert!(
+            st.values().all(|s| running(&s.borrow())),
+            "nothing moves before 15 s"
+        );
+        for id in &moving {
+            until(st.get_mut(*id).unwrap(), "handed off", standby).await;
+            let lease = leases(&store).observe(id).await.unwrap().unwrap();
+            assert!(lease.doc.released, "{id}");
+        }
+        for id in ids.iter().filter(|id| !moving.contains(id)) {
+            assert!(running(&st[*id].borrow()), "{id} stays");
+        }
+        assert!(
+            d.metrics
+                .encode()
+                .lines()
+                .any(|l| l == "data_avalanche_ring_generation 2")
+        );
+        r.shutdown().await;
+    }
+
+    /// `max_concurrent_handoffs` 1: two hand-offs whose drains hang run one after the other,
+    /// each cut off at `drain_timeout` (5 s); with 2 they run together.
+    #[tokio::test(start_paused = true)]
+    async fn the_hand_off_cap_drains_one_pipeline_at_a_time() {
+        for (cap, least, most) in [(1, 10, 11), (2, 5, 6)] {
+            let store = ScriptedStore::new();
+            let (fs, mut d) = factories(&store, &["p", "q"]);
+            d.handoffs = Arc::new(tokio::sync::Semaphore::new(cap));
+            let (tx, view) = watch::channel(alone("n1"));
+            let r =
+                ClusterReconciler::start(pipelines(&["p", "q"]), &d, view, reconciler_settings());
+            let mut st = r.statuses();
+            for s in st.values_mut() {
+                until(s, "running", running).await;
+            }
+            for (i, f) in (1..).zip(fs.values()) {
+                f.sink.hold_commits();
+                f.source
+                    .commit(FakeTxn::new().insert(&TableRef::new("public", "a"), row(i)));
+            }
+            eventually("both commits wait", || {
+                fs.values().all(|f| f.sink.waiting_commits() == 1)
+            })
+            .await;
+            let t = Instant::now();
+            tx.send_replace(members("n1", &[("n1", Phase::Draining, false)]));
+            r.released().await;
+            let took = t.elapsed();
+            assert!(
+                took >= Duration::from_secs(least) && took < Duration::from_secs(most),
+                "cap {cap}: {took:?}"
+            );
+            for f in fs.values() {
+                f.sink.release_commits();
+            }
+            r.shutdown().await;
+        }
+    }
+
+    /// A factory whose first build panics, as a bug in a controller would.
+    #[derive(Debug)]
+    struct PanicsOnce(TestFactory, AtomicBool);
+
+    impl WorkerFactory for PanicsOnce {
+        fn build(&self, p: &Pipeline) -> Result<Parts, String> {
+            assert!(self.1.swap(true, Ordering::SeqCst), "a bug");
+            self.0.build(p)
+        }
+    }
+
+    /// DA-OPS-7: a controller that panics is started again at the next resync; it takes its
+    /// own lease over under the next epoch.
+    #[tokio::test(start_paused = true)]
+    async fn a_controller_that_panics_is_started_again() {
+        let store = ScriptedStore::new();
+        let (fs, mut d) = factories(&store, &["p"]);
+        d.factory = Arc::new(PanicsOnce(fs["p"].clone(), AtomicBool::new(false)));
+        let (_, view) = watch::channel(alone("n1"));
+        let r = ClusterReconciler::start(pipelines(&["p"]), &d, view, reconciler_settings());
+        let mut st = r.statuses();
+        until(st.get_mut("p").unwrap(), "running", running).await;
+        assert_eq!(st["p"].borrow().owner_epoch, Some(2));
+        assert_eq!(fs["p"].builds(), 1, "the panicking build counted none");
+        r.shutdown().await;
+    }
 }
```

Change `crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index af902fc..cbdf7a4 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -393,6 +393,7 @@ pub(crate) fn deps(
         metrics: Metrics::new(),
         settings: settings(),
         starts: Arc::new(tokio::sync::Semaphore::new(1)),
+        handoffs: Arc::new(tokio::sync::Semaphore::new(2)),
     }
 }
 
@@ -503,6 +504,35 @@ pub(crate) fn at(n: u8) -> SocketAddr {
     SocketAddr::from(([10, 0, 0, n], 7946))
 }
 
+/// `me`'s view of the members: each `(name, phase, suspect)`, the n-th at `at(n)`.
+pub(crate) fn members(me: &str, nodes: &[(&str, crate::Phase, bool)]) -> crate::Members {
+    let nodes = nodes
+        .iter()
+        .zip(1..)
+        .map(|((name, phase, suspect), n)| {
+            let member = crate::Member {
+                addr: at(n),
+                meta: crate::NodeMeta {
+                    phase: *phase,
+                    config_generation: 1,
+                    build: "1.0.0".into(),
+                },
+                suspect: *suspect,
+            };
+            ((*name).to_owned(), member)
+        })
+        .collect();
+    crate::Members {
+        me: me.into(),
+        nodes,
+    }
+}
+
+/// `me` alone, active.
+pub(crate) fn alone(me: &str) -> crate::Members {
+    members(me, &[(me, crate::Phase::Active, false)])
+}
+
 mod hub_tests {
     use super::*;
     use crate::Wire;
```

Change `crates/data-avalanche-telemetry/tests/metrics.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/tests/metrics.rs b/crates/data-avalanche-telemetry/tests/metrics.rs
index e369478..8393757 100644
--- a/crates/data-avalanche-telemetry/tests/metrics.rs
+++ b/crates/data-avalanche-telemetry/tests/metrics.rs
@@ -148,12 +148,14 @@ fn a_pipeline_the_node_no_longer_owns_loses_its_series() {
     });
     m.owned_by(1);
     m.set_cluster_members(1);
+    m.set_ring_generation(3);
     p.remove();
     m.owned_by(-1);
     let text = m.encode();
     assert!(!text.contains("orders"), "{text}");
     assert!(text.contains("data_avalanche_owned_pipelines 0"), "{text}");
     assert!(text.contains("data_avalanche_cluster_members 1"), "{text}");
+    assert!(text.contains("data_avalanche_ring_generation 3"), "{text}");
 }
 
 #[test]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib reconciler::`

Expected: FAIL to compile: `ClusterReconciler::start` takes no members yet, `ReconcilerSettings` and `ControllerDeps.handoffs` don't exist.

```text
error[E0433]: cannot find type `Actual` in this scope
error[E0425]: cannot find type `Duration` in this scope
error[E0433]: cannot find type `Duration` in this scope
error[E0425]: cannot find type `ReconcilerSettings` in this scope
error[E0422]: cannot find struct, variant or union type `ReconcilerSettings` in this scope
error[E0433]: cannot find type `Instant` in this scope
error[E0061]: this function takes 2 arguments but 4 arguments were supplied
error[E0433]: cannot find type `ReconcilerSettings` in this scope
error[E0609]: no field `handoffs` on type `controller::ControllerDeps`
error[E0599]: no method named `released` found for struct `reconciler::ClusterReconciler` in the current scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 5cd04fb..815fbe2 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -120,6 +120,8 @@ pub struct ControllerDeps {
     pub settings: ControllerSettings,
     /// Caps the workers in Phase 0 at once (`max_concurrent_takeovers`).
     pub starts: Arc<Semaphore>,
+    /// Caps the workers handed off at once (`max_concurrent_handoffs`, Clustering §8.5).
+    pub handoffs: Arc<Semaphore>,
 }
 
 /// One pipeline's controller.
@@ -134,6 +136,7 @@ pub struct PipelineController {
     metrics: Arc<PipelineMetrics>,
     settings: ControllerSettings,
     starts: Arc<Semaphore>,
+    handoffs: Arc<Semaphore>,
     status: watch::Sender<ControllerStatus>,
 }
 
@@ -165,6 +168,7 @@ enum Event {
     Ended(Result<Result<Exit, WorkerError>, tokio::task::JoinError>),
     Status { open: bool },
     Start(OwnedSemaphorePermit),
+    HandOff(OwnedSemaphorePermit),
     Due,
     Poll,
 }
@@ -184,6 +188,14 @@ struct Running {
     reconnects: u64,
 }
 
+/// A worker never outlives its controller: a controller that panics drops its `Running`, and
+/// the worker task goes with it before the next controller claims the lease (DA-OPS-7).
+impl Drop for Running {
+    fn drop(&mut self) {
+        self.task.abort();
+    }
+}
+
 impl PipelineController {
     /// `docs` are the pipeline's state documents.
     pub fn new(
@@ -192,6 +204,17 @@ impl PipelineController {
         deps: &ControllerDeps,
     ) -> (Self, watch::Receiver<ControllerStatus>) {
         let (status, rx) = watch::channel(ControllerStatus::default());
+        (Self::reporting(pipeline, docs, deps, status), rx)
+    }
+
+    /// A controller that publishes its status into `status`: a controller started again after
+    /// its task ended keeps its predecessor's channel.
+    pub fn reporting(
+        pipeline: Pipeline,
+        docs: PipelineDocs,
+        deps: &ControllerDeps,
+        status: watch::Sender<ControllerStatus>,
+    ) -> Self {
         let labels = PipelineLabels {
             pipeline: pipeline.name.clone(),
             source_host: pipeline.source.host.clone(),
@@ -201,21 +224,19 @@ impl PipelineController {
             deps.metrics
                 .pipeline(labels, pipeline.sink.metrics.per_table),
         );
-        (
-            Self {
-                pipeline,
-                docs,
-                leases: deps.leases.clone(),
-                holder: deps.holder.clone(),
-                factory: Arc::clone(&deps.factory),
-                node: deps.metrics.clone(),
-                metrics,
-                settings: deps.settings.clone(),
-                starts: Arc::clone(&deps.starts),
-                status,
-            },
-            rx,
-        )
+        Self {
+            pipeline,
+            docs,
+            leases: deps.leases.clone(),
+            holder: deps.holder.clone(),
+            factory: Arc::clone(&deps.factory),
+            node: deps.metrics.clone(),
+            metrics,
+            settings: deps.settings.clone(),
+            starts: Arc::clone(&deps.starts),
+            handoffs: Arc::clone(&deps.handoffs),
+            status,
+        }
     }
 
     /// Runs until the target channel closes; the lease is released on the way out.
@@ -323,11 +344,20 @@ impl PipelineController {
                     _ => {}
                 }
             }
+            // "Don't own": with nothing to drain the lease goes back now; a worker is handed off
+            // once a hand-off permit is free, unless the target turns back to "own" first.
+            let hand_off = *target.borrow() == Target::DontOwn;
+            if hand_off && worker.is_none() {
+                self.give_back(run).await;
+                self.standby(None);
+                return Left::Released;
+            }
             let due = retry.as_ref().map(|(at, _)| *at);
             // A worker may run: no error, not dumped, and a desired state read that isn't
             // "stopped". Only then does a retry's time matter (else its arm would be ready at
             // every turn).
             let may_run = worker.is_none()
+                && !hand_off
                 && error.is_none()
                 && !dumped
                 && desired.is_some_and(|d| d != DesiredState::Stopped);
@@ -341,6 +371,7 @@ impl PipelineController {
                 retry.as_ref().map(|(_, e)| e.as_str()),
             );
             let starts = Arc::clone(&self.starts);
+            let handoffs = Arc::clone(&self.handoffs);
             let event = {
                 let (task, status) = match worker.as_mut() {
                     Some(Running { task, handle, .. }) => (Some(task), Some(&mut handle.status)),
@@ -365,6 +396,9 @@ impl PipelineController {
                     permit = starts.acquire_owned(), if want_start => {
                         Event::Start(permit.expect("the start semaphore isn't closed"))
                     }
+                    permit = handoffs.acquire_owned(), if hand_off => {
+                        Event::HandOff(permit.expect("the hand-off semaphore isn't closed"))
+                    }
                     () = async {
                         match due {
                             Some(at) => tokio::time::sleep_until(at).await,
@@ -376,10 +410,10 @@ impl PipelineController {
             };
             match event {
                 Event::Lost => {
-                    if let Some(w) = worker.take() {
+                    if let Some(mut w) = worker.take() {
                         w.task.abort();
                         // Ended (dropped) before the next term starts: its writer goes with it.
-                        let _ = w.task.await;
+                        let _ = (&mut w.task).await;
                         self.metrics.worker_ended();
                     }
                     let reason = match run.stop().await {
@@ -389,30 +423,15 @@ impl PipelineController {
                     tracing::error!(pipeline = %self.pipeline.name, "lease lost: {reason}");
                     return Left::Lost;
                 }
-                Event::Target { closed } => {
-                    if closed || *target.borrow() == Target::DontOwn {
-                        let drained = match worker.take() {
-                            Some(w) => self.drain(w).await,
-                            None => true,
-                        };
-                        if drained {
-                            self.give_back(run).await;
-                        } else {
-                            // Clustering §8.3: a worker cut off may have left a write in
-                            // flight; the lease expires instead of being handed over now.
-                            let _ = run.stop().await;
-                            tracing::warn!(
-                                pipeline = %self.pipeline.name,
-                                "lease left to expire after the drain was cut off"
-                            );
-                        }
-                        self.standby(None);
-                        return if closed {
-                            Left::Shutdown
-                        } else {
-                            Left::Released
-                        };
-                    }
+                // A shutdown drains at once; "don't own" waits for its hand-off permit.
+                Event::Target { closed: true } => {
+                    self.let_go(run, worker.take()).await;
+                    return Left::Shutdown;
+                }
+                Event::Target { closed: false } => {}
+                Event::HandOff(_permit) => {
+                    self.let_go(run, worker.take()).await;
+                    return Left::Released;
                 }
                 Event::Ended(ended) => {
                     let mut w = worker.take().expect("a worker ended");
@@ -574,6 +593,26 @@ impl PipelineController {
         drained
     }
 
+    /// Drains the worker and releases the lease; a worker cut off leaves it to expire.
+    async fn let_go(&self, run: LeaseRun, worker: Option<Running>) {
+        let drained = match worker {
+            Some(w) => self.drain(w).await,
+            None => true,
+        };
+        if drained {
+            self.give_back(run).await;
+        } else {
+            // Clustering §8.3: a worker cut off may have left a write in flight; the lease
+            // expires instead of being handed over now.
+            let _ = run.stop().await;
+            tracing::warn!(
+                pipeline = %self.pipeline.name,
+                "lease left to expire after the drain was cut off"
+            );
+        }
+        self.standby(None);
+    }
+
     async fn give_back(&self, run: LeaseRun) {
         match run.stop().await {
             RenewEnd::Stopped(lease) => match release(&self.leases, &lease).await {
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index a26bd6f..69eae8c 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -1,7 +1,7 @@
-//! Who runs each pipeline (Clustering §19–§20): the lease, its renewer and the commit guard it
-//! answers, the controller that runs a pipeline's worker while it holds the lease, and the
-//! reconciler that tells each controller whether to own. M6b runs them single-node: a self-only
-//! ring, so every pipeline's target is "own".
+//! Who runs each pipeline (Clustering §2–§8, §19–§20): the gossip that tells a node who its
+//! peers are, the ring that places each pipeline on one of them, the lease, its renewer and the
+//! commit guard it answers, the controller that runs a pipeline's worker while it holds the
+//! lease, and the reconciler that tells each controller whether to own.
 
 pub mod claim;
 pub mod controller;
@@ -22,6 +22,6 @@ pub use controller::{
 pub use gossip::{Gossip, GossipSettings, Member, Members, NodeIdentity, NodeMeta, Phase};
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
 pub use placement::Placement;
-pub use reconciler::{ClusterReconciler, node_identity};
+pub use reconciler::{ClusterReconciler, ReconcilerSettings, node_identity};
 pub use ring::Ring;
 pub use wire::{GossipKey, Udp, Wire};
```

Change `crates/data-avalanche-cluster/src/reconciler.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/reconciler.rs b/crates/data-avalanche-cluster/src/reconciler.rs
index db9389c..16b8d9e 100644
--- a/crates/data-avalanche-cluster/src/reconciler.rs
+++ b/crates/data-avalanche-cluster/src/reconciler.rs
@@ -1,16 +1,19 @@
 //! The node's reconciler (Clustering §8.1, §20). It runs one controller per pipeline and tells
-//! each whether to own its pipeline. M6b's ring holds this node alone, so one tick sets every
-//! target to "own"; M8 derives the targets from membership.
+//! each whether to own its pipeline: "own" when the ring over the members places the pipeline on
+//! this node. It never touches a lease; each controller claims and lets go of its own.
 
 use std::collections::BTreeMap;
-use std::time::{SystemTime, UNIX_EPOCH};
+use std::time::{Duration, SystemTime, UNIX_EPOCH};
 
 use data_avalanche_config::{Cluster, Pipeline};
 use data_avalanche_state::{Holder, PipelineDocs};
-use tokio::sync::watch;
+use tokio::sync::{oneshot, watch};
 use tokio::task::JoinHandle;
+use tokio::time::{Instant, MissedTickBehavior};
 
-use crate::{ControllerDeps, ControllerStatus, PipelineController, Target};
+use crate::{
+    Actual, ControllerDeps, ControllerStatus, Members, PipelineController, Placement, Target,
+};
 
 /// This node as a lease holder (Clustering §3.4, D-1): `node_id` is
 /// `globals.cluster.memberlist.node_name`, else the OS hostname; the incarnation is the process's
@@ -38,68 +41,204 @@ pub fn node_identity(cluster: &Cluster, started: SystemTime) -> Result<Holder, S
     })
 }
 
+/// The reconciler's settings, from `globals.cluster`.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct ReconcilerSettings {
+    pub tokens: u32,
+    pub stabilization: Duration,
+    /// Targets are recomputed, and ended controllers started again, at least this often
+    /// (`lease_renewal_interval`).
+    pub resync: Duration,
+}
+
+impl ReconcilerSettings {
+    pub fn new(cluster: &Cluster) -> Self {
+        Self {
+            tokens: cluster.tokens,
+            stabilization: cluster.rebalance_stabilization,
+            resync: cluster.lease_renewal_interval,
+        }
+    }
+}
+
 #[derive(Debug)]
 struct Controlled {
+    pipeline: Pipeline,
+    docs: PipelineDocs,
     target: watch::Sender<Target>,
-    status: watch::Receiver<ControllerStatus>,
+    status: watch::Sender<ControllerStatus>,
     task: JoinHandle<()>,
 }
 
-/// The controllers of every configured pipeline.
+/// A controller dropped is aborted, its worker with it: a crash, its lease left to expire.
+impl Drop for Controlled {
+    fn drop(&mut self) {
+        self.task.abort();
+    }
+}
+
+impl Controlled {
+    fn spawn(&mut self, deps: &ControllerDeps) {
+        let controller = PipelineController::reporting(
+            self.pipeline.clone(),
+            self.docs.clone(),
+            deps,
+            self.status.clone(),
+        );
+        self.task = tokio::spawn(controller.run(self.target.subscribe()));
+    }
+}
+
+/// The controllers of every configured pipeline, and the task that sets their targets from the
+/// ring. Dropped without a [`Self::shutdown`], it stops as a crash would: every controller and
+/// worker is aborted and every lease is left to expire.
 #[derive(Debug)]
 pub struct ClusterReconciler {
-    controllers: BTreeMap<String, Controlled>,
+    statuses: BTreeMap<String, watch::Receiver<ControllerStatus>>,
+    stop: Option<oneshot::Sender<()>>,
+    task: JoinHandle<()>,
+}
+
+impl Drop for ClusterReconciler {
+    fn drop(&mut self) {
+        self.task.abort();
+    }
 }
 
 impl ClusterReconciler {
-    /// Starts a controller for each pipeline (with its state documents) and runs one reconcile
-    /// tick of a self-only ring: every pipeline's target is "own".
-    pub fn start(pipelines: Vec<(Pipeline, PipelineDocs)>, deps: &ControllerDeps) -> Self {
-        deps.metrics.set_cluster_members(1);
+    /// Starts a controller for each pipeline (with its state documents), its target "don't own"
+    /// until the ring over `members` gives it to this node.
+    pub fn start(
+        pipelines: Vec<(Pipeline, PipelineDocs)>,
+        deps: &ControllerDeps,
+        members: watch::Receiver<Members>,
+        settings: ReconcilerSettings,
+    ) -> Self {
+        let mut statuses = BTreeMap::new();
         let controllers = pipelines
             .into_iter()
             .map(|(pipeline, docs)| {
                 let id = pipeline.id.clone();
-                let (controller, status) = PipelineController::new(pipeline, docs, deps);
-                let (target, rx) = watch::channel(Target::Own);
-                let task = tokio::spawn(controller.run(rx));
-                (
-                    id,
-                    Controlled {
-                        target,
-                        status,
-                        task,
-                    },
-                )
+                let (status, rx) = watch::channel(ControllerStatus::default());
+                statuses.insert(id.clone(), rx);
+                let mut c = Controlled {
+                    pipeline,
+                    docs,
+                    target: watch::channel(Target::DontOwn).0,
+                    status,
+                    task: tokio::spawn(async {}),
+                };
+                c.spawn(deps);
+                (id, c)
             })
             .collect();
-        Self { controllers }
+        let (stop, stopped) = oneshot::channel();
+        let task = tokio::spawn(reconcile(
+            controllers,
+            deps.clone(),
+            members,
+            settings,
+            stopped,
+        ));
+        Self {
+            statuses,
+            stop: Some(stop),
+            task,
+        }
     }
 
     /// Each pipeline's controller status, by pipeline id.
     pub fn statuses(&self) -> BTreeMap<String, watch::Receiver<ControllerStatus>> {
-        self.controllers
-            .iter()
-            .map(|(id, c)| (id.clone(), c.status.clone()))
-            .collect()
+        self.statuses.clone()
+    }
+
+    /// Waits until this node holds no pipeline: every controller is on standby.
+    pub async fn released(&self) {
+        for status in self.statuses.values() {
+            let mut status = status.clone();
+            let _ = status
+                .wait_for(|s| matches!(s.actual, Some(Actual::Standby { .. })))
+                .await;
+        }
     }
 
     /// Lets every pipeline go: each controller drains its worker (within `drain_timeout`) and
     /// releases its lease, so the next process acquires it without waiting.
-    pub async fn shutdown(self) {
-        let tasks: Vec<JoinHandle<()>> = self
-            .controllers
-            .into_values()
-            .map(|c| {
-                drop(c.target);
-                c.task
-            })
-            .collect();
-        for task in tasks {
-            if let Err(e) = task.await {
-                tracing::error!("a pipeline controller ended abnormally: {e}");
+    pub async fn shutdown(mut self) {
+        if let Some(stop) = self.stop.take() {
+            let _ = stop.send(());
+        }
+        if let Err(e) = (&mut self.task).await {
+            tracing::error!("the cluster reconciler ended abnormally: {e}");
+        }
+    }
+}
+
+/// The reconcile loop (Clustering §8.1, §20.1): on every membership change, every due ring
+/// addition and every `resync`, each pipeline's target is "own" iff the ring places it here.
+async fn reconcile(
+    mut controllers: BTreeMap<String, Controlled>,
+    deps: ControllerDeps,
+    mut members: watch::Receiver<Members>,
+    settings: ReconcilerSettings,
+    mut stop: oneshot::Receiver<()>,
+) {
+    let mut placement = Placement::new(settings.tokens, settings.stabilization);
+    let mut resync = tokio::time::interval(settings.resync);
+    resync.set_missed_tick_behavior(MissedTickBehavior::Delay);
+    let mut gossiping = true;
+    loop {
+        let view = members.borrow_and_update().clone();
+        if placement.update(&view, Instant::now()) {
+            tracing::info!(
+                nodes = ?placement.ring().nodes(),
+                generation = placement.generation(),
+                "the ring changed"
+            );
+        }
+        deps.metrics.set_cluster_members(view.nodes.len());
+        deps.metrics.set_ring_generation(placement.generation());
+        for (id, c) in &mut controllers {
+            let want = if placement.ring().owner(id) == Some(view.me.as_str()) {
+                Target::Own
+            } else {
+                Target::DontOwn
+            };
+            c.target
+                .send_if_modified(|t| std::mem::replace(t, want) != want);
+            // A controller ends only when its target closes: one that ended panicked
+            // (DA-OPS-7). Its worker went with it; the next one takes the lease over.
+            if c.task.is_finished() {
+                tracing::error!(
+                    pipeline = %c.pipeline.name,
+                    "the pipeline's controller ended unasked; it is started again"
+                );
+                c.spawn(&deps);
             }
         }
+        let due = placement.next_due();
+        tokio::select! {
+            _ = &mut stop => break,
+            changed = members.changed(), if gossiping => gossiping = changed.is_ok(),
+            () = async {
+                match due {
+                    Some(at) => tokio::time::sleep_until(at).await,
+                    None => std::future::pending().await,
+                }
+            } => {}
+            _ = resync.tick() => {}
+        }
+    }
+    // Every target closes at once (a replaced sender is dropped): the controllers drain
+    // together.
+    let mut controllers: Vec<Controlled> = controllers.into_values().collect();
+    for c in &mut controllers {
+        c.target = watch::channel(Target::DontOwn).0;
+    }
+    for c in &mut controllers {
+        if let Err(e) = (&mut c.task).await {
+            tracing::error!("a pipeline controller ended abnormally: {e}");
+        }
     }
 }
 
```

Change `crates/data-avalanche-telemetry/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/src/lib.rs b/crates/data-avalanche-telemetry/src/lib.rs
index ad2539d..15eaf89 100644
--- a/crates/data-avalanche-telemetry/src/lib.rs
+++ b/crates/data-avalanche-telemetry/src/lib.rs
@@ -131,6 +131,7 @@ struct Families {
     lease_renew_latency: HistogramFamily,
     owned_pipelines: Gauge,
     cluster_members: Gauge,
+    ring_generation: Gauge,
     wal_lag_bytes: Family<Labels, Gauge>,
     source_reconnects: Family<Labels, Counter>,
     snapshot_rows_done: Family<Labels, Gauge>,
@@ -168,6 +169,7 @@ impl Metrics {
             lease_renew_latency: Family::new_with_constructor(renew_latency),
             owned_pipelines: Gauge::default(),
             cluster_members: Gauge::default(),
+            ring_generation: Gauge::default(),
             wal_lag_bytes: Family::default(),
             source_reconnects: Family::default(),
             snapshot_rows_done: Family::default(),
@@ -209,6 +211,11 @@ impl Metrics {
             "Members of the cluster this node sees",
             f.cluster_members.clone(),
         );
+        r.register(
+            "ring_generation",
+            "Changes of the ring's nodes since this node started",
+            f.ring_generation.clone(),
+        );
         r.register(
             "wal_lag_bytes",
             "WAL the pipeline's replication slot retains",
@@ -271,6 +278,10 @@ impl Metrics {
         self.families.cluster_members.set(gauge(n as u64));
     }
 
+    pub fn set_ring_generation(&self, generation: u64) {
+        self.families.ring_generation.set(gauge(generation));
+    }
+
     /// The handle that sets one pipeline's series. `per_table` is `sink.metrics.per_table`.
     pub fn pipeline(&self, labels: PipelineLabels, per_table: bool) -> PipelineMetrics {
         PipelineMetrics {
```

Change `crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 9977748..98f1677 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -6,7 +6,8 @@ use std::sync::atomic::{AtomicBool, Ordering};
 use std::time::{Duration, SystemTime};
 
 use data_avalanche_cluster::{
-    ClusterReconciler, ControllerDeps, ControllerSettings, LeaseTiming, node_identity,
+    ClusterReconciler, ControllerDeps, ControllerSettings, LeaseTiming, Member, Members, NodeMeta,
+    Phase, ReconcilerSettings, node_identity,
 };
 use data_avalanche_config::{
     Diagnostic, FingerprintKey, ProcessEnv, Report, ResolvedConfig, Severity, load_str,
@@ -15,7 +16,7 @@ use data_avalanche_pipeline::{Backoff, Tuning};
 use data_avalanche_state::Leases;
 use data_avalanche_telemetry::Metrics;
 use thiserror::Error;
-use tokio::sync::{Semaphore, oneshot};
+use tokio::sync::{Semaphore, oneshot, watch};
 
 use crate::http::{self, Http};
 use crate::startup::{Buckets, join};
@@ -145,12 +146,30 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         starts: Arc::new(Semaphore::new(
             usize::try_from(c.max_concurrent_takeovers).unwrap_or(usize::MAX),
         )),
+        handoffs: Arc::new(Semaphore::new(
+            usize::try_from(c.max_concurrent_handoffs).unwrap_or(usize::MAX),
+        )),
+    };
+    // This node alone until the gossip runs.
+    let me = Member {
+        addr: ([0, 0, 0, 0], 0).into(),
+        meta: NodeMeta {
+            phase: Phase::Active,
+            config_generation: 0,
+            build: env!("CARGO_PKG_VERSION").to_owned(),
+        },
+        suspect: false,
     };
+    let (_alone, members) = watch::channel(Members {
+        me: deps.holder.node_id.clone(),
+        nodes: [(deps.holder.node_id.clone(), me)].into(),
+    });
     let mut pipelines = Vec::new();
     for p in config.pipelines.values() {
         pipelines.push((p.clone(), buckets.pipeline_docs(p)?));
     }
-    let reconciler = ClusterReconciler::start(pipelines, &deps);
+    let reconciler =
+        ClusterReconciler::start(pipelines, &deps, members, ReconcilerSettings::new(c));
     ready.store(true, Ordering::SeqCst);
     shutdown_signal().await;
     tracing::info!("shutting down: draining every pipeline and releasing its lease");
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib reconciler::`

Expected: PASS, 8 tests.

```text
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 61 filtered out; finished in 0.28s
```

Run: `cargo test -p data-avalanche-telemetry`

Expected: PASS, 6 tests.

```text
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1121 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/controller.rs \
  crates/data-avalanche-cluster/src/lib.rs \
  crates/data-avalanche-cluster/src/placement.rs \
  crates/data-avalanche-cluster/src/reconciler.rs \
  crates/data-avalanche-cluster/src/test_support.rs \
  crates/data-avalanche-telemetry/src/lib.rs \
  crates/data-avalanche-telemetry/tests/metrics.rs \
  crates/data-avalanche/src/run.rs
git commit -m "feat(cluster): the reconciler on the ring — targets from placement over the members, the hand-off cap (max_concurrent_handoffs), a controller that ended unasked started again (DA-OPS-7), a drop is a crash"
```

---

### Task 7: The leave (`data-avalanche-cluster`)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/gossip.rs`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`
- Create: `crates/data-avalanche-cluster/src/node.rs`
- Modify: `crates/data-avalanche-cluster/src/reconciler.rs`
- Modify (test): `crates/data-avalanche-cluster/src/test_support.rs`

**Interfaces:**
- Consumes: `Gossip::{set_phase, leave, members}` (Task 4); `ClusterReconciler::{released, shutdown}` (Task 6 and here).
- Produces:
  - `node::leave(gossip: Gossip, reconciler: ClusterReconciler)` (async; re-exported as `leave`)
  - `ClusterReconciler::released(&self)` (async): every controller is on standby
  - `test_support`: `TestCluster { hub, store, factories, ids, docs }` with `async start(&self, name, n, incarnation, seed: Option<SocketAddr>) -> TestNode`, `insert`, `landed`; `TestNode { name, gossip, reconciler }`; `settles(what, nodes, placed)`, `runners(nodes)`

D-M8a.13, Clustering §8.3. A node with peers sets `Draining`, which takes it off every ring at once (Task 5). Its controllers' targets turn `DontOwn`, so each pipeline is drained under the hand-off cap and its lease released. The new owner, already claiming, acquires it as soon as it is released, without waiting out the lease. Once every controller is on standby the node sets `Leaving`, shuts the reconciler down (draining whatever a late claim gave it, uncapped) and leaves the gossip, which the others see at once. A lone node has no one to hand to: its reconciler shuts down, releasing every lease for the next process. `TestCluster` runs several nodes in one process: one hub, one in-memory store, a recording factory per node. Task 10 uses it for the cluster scenarios.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-cluster/src/gossip.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/gossip.rs b/crates/data-avalanche-cluster/src/gossip.rs
index 18fca90..bddae95 100644
--- a/crates/data-avalanche-cluster/src/gossip.rs
+++ b/crates/data-avalanche-cluster/src/gossip.rs
@@ -431,18 +431,7 @@ fn log(r: Result<(), foca::Error>) {
 #[cfg(test)]
 mod tests {
     use super::*;
-    use crate::test_support::{Hub, at, eventually};
-
-    fn settings() -> GossipSettings {
-        GossipSettings {
-            probe_interval: Duration::from_secs(1),
-            probe_timeout: Duration::from_millis(500),
-            gossip_interval: Duration::from_millis(200),
-            gossip_nodes: 3,
-            push_pull_interval: Duration::from_secs(5),
-            retransmit_factor: 4,
-        }
-    }
+    use crate::test_support::{Hub, at, eventually, gossip_settings as settings};
 
     fn identity(name: &str, incarnation: u64, addr: SocketAddr) -> NodeIdentity {
         NodeIdentity {
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 69eae8c..f2769de 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -7,6 +7,7 @@ pub mod claim;
 pub mod controller;
 pub mod gossip;
 pub mod lease;
+pub mod node;
 pub mod placement;
 pub mod reconciler;
 pub mod ring;
```

Create `crates/data-avalanche-cluster/src/node.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/node.rs b/crates/data-avalanche-cluster/src/node.rs
new file mode 100644
index 0000000..92c285c
--- /dev/null
+++ b/crates/data-avalanche-cluster/src/node.rs
@@ -0,0 +1,64 @@
+#[cfg(test)]
+mod tests {
+    use std::time::Duration;
+
+    use tokio::time::Instant;
+
+    use super::*;
+    use crate::Ring;
+    use crate::test_support::{STABLE, TestCluster, at, eventually, leases, settles, timing};
+
+    const IDS: [&str; 4] = ["orders", "events", "customers", "users"];
+
+    /// `a` leaves a two-node cluster: `b` runs everything within a lease renewal or two, not
+    /// after a lease_duration, and no pipeline ever runs on both.
+    #[tokio::test(start_paused = true)]
+    async fn a_leave_hands_every_pipeline_off_without_a_lease_wait() {
+        let c = TestCluster::new(&IDS);
+        let a = c.start("a", 1, 1, None);
+        settles("all on a", &[&a], |_| "a".into()).await;
+        let b = c.start("b", 2, 1, Some(at(1)));
+        let two = Ring::new(["a", "b"], 128);
+        tokio::time::sleep(STABLE).await;
+        settles("spread over a and b", &[&a, &b], |id| {
+            two.owner(id).unwrap().into()
+        })
+        .await;
+        c.insert(1);
+        eventually("1 lands", || c.landed(&[1])).await;
+        let t = Instant::now();
+        let left = tokio::spawn(leave(a.gossip, a.reconciler));
+        settles("all on b", &[&b], |_| "b".into()).await;
+        let took = t.elapsed();
+        assert!(took < timing().duration, "{took:?}: a lease wait");
+        left.await.unwrap();
+        for id in IDS {
+            let lease = leases(&c.store).observe(id).await.unwrap().unwrap();
+            assert_eq!(lease.doc.holder_node_id, "b", "{id}");
+        }
+        c.insert(2);
+        eventually("2 lands", || c.landed(&[1, 2])).await;
+        assert_eq!(
+            b.gossip.members().borrow().peers(),
+            0,
+            "a is gone from b's members"
+        );
+    }
+
+    /// A lone node's leave releases every lease: the next process acquires them at once.
+    #[tokio::test(start_paused = true)]
+    async fn a_lone_node_leaves_by_releasing_its_leases() {
+        let c = TestCluster::new(&IDS);
+        let a = c.start("a", 1, 1, None);
+        settles("all on a", &[&a], |_| "a".into()).await;
+        leave(a.gossip, a.reconciler).await;
+        for id in IDS {
+            let lease = leases(&c.store).observe(id).await.unwrap().unwrap();
+            assert!(lease.doc.released, "{id}");
+        }
+        let a2 = c.start("a", 1, 2, None);
+        let t = Instant::now();
+        settles("all on the next a", &[&a2], |_| "a".into()).await;
+        assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
+    }
+}
```

Change `crates/data-avalanche-cluster/src/reconciler.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/reconciler.rs b/crates/data-avalanche-cluster/src/reconciler.rs
index 16b8d9e..8a0c2a9 100644
--- a/crates/data-avalanche-cluster/src/reconciler.rs
+++ b/crates/data-avalanche-cluster/src/reconciler.rs
@@ -252,21 +252,11 @@ mod tests {
 
     use super::*;
     use crate::test_support::{
-        ScriptedStore, TestFactory, alone, config, deps, eventually, leases, members,
-        pipeline_docs, row, until,
+        ByPipeline, STABLE, ScriptedStore, TestFactory, alone, config, deps, eventually, leases,
+        members, pipeline_docs, row, until,
     };
     use crate::{Parts, Phase, Ring, WorkerFactory};
 
-    /// One [`TestFactory`] per pipeline id.
-    #[derive(Debug)]
-    struct ByPipeline(BTreeMap<String, TestFactory>);
-
-    impl WorkerFactory for ByPipeline {
-        fn build(&self, p: &Pipeline) -> Result<Parts, String> {
-            self.0[&p.id].build(p)
-        }
-    }
-
     fn running(s: &ControllerStatus) -> bool {
         s.actual == Some(Actual::Running)
     }
@@ -275,8 +265,6 @@ mod tests {
         matches!(s.actual, Some(Actual::Standby { .. }))
     }
 
-    const STABLE: Duration = Duration::from_secs(15);
-
     fn reconciler_settings() -> ReconcilerSettings {
         ReconcilerSettings {
             tokens: 128,
```

Change `crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index cbdf7a4..baa8158 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -553,3 +553,169 @@ mod hub_tests {
         a.send(at(9), Bytes::from_static(b"4"));
     }
 }
+
+/// The gossip's timers in the tests: config §6.5.1's defaults, a 5 s push/pull.
+pub(crate) fn gossip_settings() -> crate::GossipSettings {
+    crate::GossipSettings {
+        probe_interval: Duration::from_secs(1),
+        probe_timeout: Duration::from_millis(500),
+        gossip_interval: Duration::from_millis(200),
+        gossip_nodes: 3,
+        push_pull_interval: Duration::from_secs(5),
+        retransmit_factor: 4,
+    }
+}
+
+/// One [`TestFactory`] per pipeline id. The nodes of a [`TestCluster`] share it: a pipeline's
+/// source and sink are the same whichever node runs it, as a database and a lake are.
+#[derive(Debug, Clone)]
+pub(crate) struct ByPipeline(pub std::collections::BTreeMap<String, TestFactory>);
+
+impl crate::WorkerFactory for ByPipeline {
+    fn build(&self, p: &data_avalanche_config::Pipeline) -> Result<crate::Parts, String> {
+        self.0[&p.id].build(p)
+    }
+}
+
+/// `rebalance_stabilization` in the cluster tests.
+pub(crate) const STABLE: Duration = Duration::from_secs(15);
+
+/// In-process nodes: their gossip on one hub, their leases in one store, their pipelines'
+/// sources, sinks and state documents shared.
+#[derive(Debug)]
+pub(crate) struct TestCluster {
+    pub hub: Arc<Hub>,
+    pub store: Arc<ScriptedStore>,
+    pub factories: ByPipeline,
+    ids: Vec<String>,
+    docs: std::collections::BTreeMap<String, data_avalanche_state::PipelineDocs>,
+}
+
+/// One node of a [`TestCluster`]. Dropped, it is a crash.
+#[derive(Debug)]
+pub(crate) struct TestNode {
+    pub name: String,
+    pub gossip: crate::Gossip,
+    pub reconciler: crate::ClusterReconciler,
+}
+
+impl TestCluster {
+    pub fn new(ids: &[&str]) -> Self {
+        Self {
+            hub: Hub::new(),
+            store: ScriptedStore::new(),
+            factories: ByPipeline(
+                ids.iter()
+                    .map(|id| ((*id).to_owned(), TestFactory::new(&["a"])))
+                    .collect(),
+            ),
+            ids: ids.iter().map(|id| (*id).to_owned()).collect(),
+            docs: ids
+                .iter()
+                .map(|id| ((*id).to_owned(), pipeline_docs()))
+                .collect(),
+        }
+    }
+
+    /// Starts `name` at `at(n)` as its `incarnation`, active at once, announcing itself to
+    /// `seed`.
+    pub fn start(&self, name: &str, n: u8, incarnation: u64, seed: Option<SocketAddr>) -> TestNode {
+        let (port, inbox) = self.hub.port(at(n));
+        let me = crate::NodeIdentity {
+            node_id: name.to_owned(),
+            incarnation,
+            rev: 0,
+            addr: at(n),
+            meta: crate::NodeMeta {
+                phase: crate::Phase::Active,
+                config_generation: 1,
+                build: "1.0.0".into(),
+            },
+        };
+        let key = crate::GossipKey::derive(&[7; 32]);
+        let gossip = crate::Gossip::start(me, key, gossip_settings(), Arc::new(port), inbox);
+        if let Some(seed) = seed {
+            gossip.announce(vec![seed]);
+        }
+        let mut deps = deps(
+            name,
+            &self.store,
+            self.factories.0.values().next().expect("a pipeline"),
+        );
+        deps.holder.incarnation = incarnation;
+        deps.factory = Arc::new(self.factories.clone());
+        deps.starts = Arc::new(tokio::sync::Semaphore::new(4));
+        let ids: Vec<&str> = self.ids.iter().map(String::as_str).collect();
+        let mut cfg = config(&ids).pipelines;
+        let pipelines = self
+            .ids
+            .iter()
+            .map(|id| (cfg.remove(id).expect("configured"), self.docs[id].clone()))
+            .collect();
+        let settings = crate::ReconcilerSettings {
+            tokens: 128,
+            stabilization: STABLE,
+            resync: timing().renewal_interval,
+        };
+        let reconciler =
+            crate::ClusterReconciler::start(pipelines, &deps, gossip.members(), settings);
+        TestNode {
+            name: name.to_owned(),
+            gossip,
+            reconciler,
+        }
+    }
+
+    /// Commits a transaction inserting `id` into every pipeline's `public.a`.
+    pub fn insert(&self, id: i64) {
+        for f in self.factories.0.values() {
+            f.source.commit(
+                data_avalanche_testkit::FakeTxn::new()
+                    .insert(&data_avalanche_core::TableRef::new("public", "a"), row(id)),
+            );
+        }
+    }
+
+    /// True when every pipeline's sink holds exactly `ids`.
+    pub fn landed(&self, ids: &[i64]) -> bool {
+        self.factories.0.values().all(|f| f.ids("a") == ids)
+    }
+}
+
+/// The nodes each pipeline's worker runs on (starting or running), by pipeline id.
+pub(crate) fn runners(nodes: &[&TestNode]) -> std::collections::BTreeMap<String, Vec<String>> {
+    use crate::Actual;
+    let mut by: std::collections::BTreeMap<String, Vec<String>> = Default::default();
+    for n in nodes {
+        for (id, s) in n.reconciler.statuses() {
+            let runs = matches!(s.borrow().actual, Some(Actual::Starting | Actual::Running));
+            let names = by.entry(id).or_default();
+            if runs {
+                names.push(n.name.clone());
+            }
+        }
+    }
+    by
+}
+
+/// Waits (on the test clock) until every pipeline runs on one of `nodes` as `placed` says,
+/// checking at every step that no pipeline runs on two nodes.
+pub(crate) async fn settles(what: &str, nodes: &[&TestNode], placed: impl Fn(&str) -> String) {
+    for _ in 0..12_000 {
+        let now = runners(nodes);
+        for (id, on) in &now {
+            assert!(
+                on.len() <= 1,
+                "{id} runs on {on:?} while waiting until {what}"
+            );
+        }
+        if now
+            .iter()
+            .all(|(id, on)| on.len() == 1 && on[0] == placed(id))
+        {
+            return;
+        }
+        tokio::time::sleep(Duration::from_millis(10)).await;
+    }
+    panic!("timed out waiting until {what}: {:?}", runners(nodes));
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib node::`

Expected: FAIL to compile: `node::leave` doesn't exist.

```text
error[E0425]: cannot find function `leave` in this scope  --> crates/data-avalanche-cluster/src/node.rs:30:33
error[E0425]: cannot find function `leave` in this scope  --> crates/data-avalanche-cluster/src/node.rs:54:9
error: could not compile `data-avalanche-cluster` (lib test) due to 2 previous errors; 1 warning emitted
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index f2769de..da781a5 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -22,6 +22,7 @@ pub use controller::{
 };
 pub use gossip::{Gossip, GossipSettings, Member, Members, NodeIdentity, NodeMeta, Phase};
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
+pub use node::leave;
 pub use placement::Placement;
 pub use reconciler::{ClusterReconciler, ReconcilerSettings, node_identity};
 pub use ring::Ring;
```

Change `crates/data-avalanche-cluster/src/node.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/node.rs b/crates/data-avalanche-cluster/src/node.rs
index 92c285c..7ce0c65 100644
--- a/crates/data-avalanche-cluster/src/node.rs
+++ b/crates/data-avalanche-cluster/src/node.rs
@@ -1,3 +1,27 @@
+//! A node's way out of the cluster (Clustering §3.5, §8.3).
+
+use crate::{ClusterReconciler, Gossip, Phase};
+
+/// Leaves the cluster on a shutdown. With peers, the node drains first (Clustering §8.3): its
+/// phase becomes `Draining`, which takes it off every ring at once, so each pipeline it runs is
+/// handed off (`max_concurrent_handoffs` at a time) and released, and its new owner acquires it
+/// without waiting out the lease; then `Leaving`, and the gossip's leave, which the others see
+/// at once rather than after a suspicion. A node without peers has no one to hand to: its
+/// pipelines drain together and release their leases for the next process.
+pub async fn leave(gossip: Gossip, reconciler: ClusterReconciler) {
+    let peers = gossip.members().borrow().peers();
+    if peers > 0 {
+        tracing::info!(peers, "leaving the cluster: handing every pipeline off");
+        gossip.set_phase(Phase::Draining);
+        reconciler.released().await;
+        gossip.set_phase(Phase::Leaving);
+    }
+    // What is still held (a claim that landed during the drain) drains here, uncapped.
+    reconciler.shutdown().await;
+    gossip.leave().await;
+    tracing::info!("left the cluster");
+}
+
 #[cfg(test)]
 mod tests {
     use std::time::Duration;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib node::`

Expected: PASS, 2 tests.

```text
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 69 filtered out; finished in 0.45s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1123 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/gossip.rs \
  crates/data-avalanche-cluster/src/lib.rs \
  crates/data-avalanche-cluster/src/node.rs \
  crates/data-avalanche-cluster/src/reconciler.rs \
  crates/data-avalanche-cluster/src/test_support.rs
git commit -m "feat(cluster): the leave — draining takes the node off every ring, its pipelines handed off and released, then the gossip's leave; an in-process cluster for the tests"
```

---

### Task 8: A lake ahead of the lease (`data-avalanche-state`, `data-avalanche-cluster`; DA-CLUS-14)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/claim.rs`
- Modify: `crates/data-avalanche-cluster/src/controller.rs`
- Modify: `crates/data-avalanche-state/src/lease.rs`

**Interfaces:**
- Consumes: M2's `Leases::take_over`; M6b's `Claim`, `Claimed`, the controller's `Left`.
- Produces:
  - `Leases::take_over_at(&self, observed: &Versioned<LeaseDoc>, holder: &Holder, lease_duration: Duration, now: DateTime<Utc>, at_least: u64) -> Result<Versioned<LeaseDoc>, StateError>`: the epoch is `max(observed.epoch + 1, at_least)`. `take_over` calls it with 0.
  - `Claim::at_least(&mut self, epoch: u64)`
  - `Claimed::Mine { lease, written, jumped: bool }` (`jumped`: the takeover landed above the next epoch)

D-M8a.14. M6b's controller met a worker fenced by a table's `current_epoch` (the lake ahead of the lease store) by claiming again. The claim took its own lease over at the stored epoch + 1, which was still behind the lake, so the worker climbed one run per epoch (DA-CLUS-14). Now the fence's epoch goes into the claim as a floor. Its own lease is taken over at `max(stored + 1, current_epoch + 1)` in one step. The floor never touches another holder's lease (that node's claim is its own business), and it is kept until the takeover lands. A second fence after a jumped takeover can only mean another live lease store writing the same lake. Climbing again would defeat the fence, so the pipeline goes to `ERROR`, naming the disagreement and keeping its lease, with no halt code.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-cluster/src/claim.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/claim.rs b/crates/data-avalanche-cluster/src/claim.rs
index 12fdddb..8020426 100644
--- a/crates/data-avalanche-cluster/src/claim.rs
+++ b/crates/data-avalanche-cluster/src/claim.rs
@@ -257,6 +257,46 @@ mod tests {
         assert!(stored.doc.is_held_by(&holder("b")));
     }
 
+    /// DA-CLUS-14: the lake saw epoch 5 while this node's own lease names 1. The takeover
+    /// jumps to 6, and keeps the floor through a write that failed.
+    #[tokio::test(start_paused = true)]
+    async fn its_own_lease_is_taken_over_at_the_floor() {
+        let store = ScriptedStore::new();
+        acquired(&leases(&store), "a").await;
+        let mut a = claim(&store, "a");
+        a.at_least(6);
+        store.fail_next(Fault {
+            kind: Kind::Transient,
+            lands: false,
+        });
+        assert!(a.step().await.is_err());
+        let jumped = a.step().await.unwrap();
+        assert!(
+            matches!(jumped, Claimed::Mine { jumped: true, .. }),
+            "{jumped:?}"
+        );
+        assert_eq!(epoch(&jumped), 6);
+        // Spent: the next own takeover is the next epoch.
+        let next = a.step().await.unwrap();
+        assert!(
+            matches!(next, Claimed::Mine { jumped: false, .. }),
+            "{next:?}"
+        );
+        assert_eq!(epoch(&next), 7);
+    }
+
+    /// The floor is for this process's own lease: another holder's is claimed as ever.
+    #[tokio::test(start_paused = true)]
+    async fn the_floor_leaves_another_holders_lease_alone() {
+        let store = ScriptedStore::new();
+        acquired(&leases(&store), "a").await;
+        let mut b = claim(&store, "b");
+        b.at_least(9);
+        assert!(matches!(b.step().await.unwrap(), Claimed::HeldBy { .. }));
+        advance(15_000).await;
+        assert_eq!(epoch(&b.step().await.unwrap()), 2);
+    }
+
     #[tokio::test(start_paused = true)]
     async fn a_takeover_that_landed_behind_an_error_is_this_nodes() {
         let store = ScriptedStore::new();
```

Change `crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 815fbe2..d293d37 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -1039,10 +1039,12 @@ mod tests {
         );
     }
 
-    /// A worker that ends for the lease (a newer epoch wrote a table) goes back to claiming: the
-    /// next term runs under a new epoch, never `ERROR`.
+    /// A worker that ends for the lease (a newer epoch wrote a table) goes back to claiming. The
+    /// lease still names this node, at epoch 1, while the table was written at 5 (a moved or
+    /// restored `globals.state`, DA-CLUS-14): the next term jumps to 6 at once, not one epoch
+    /// per worker run.
     #[tokio::test(start_paused = true)]
-    async fn a_worker_fenced_goes_back_to_the_lease() {
+    async fn a_worker_fenced_goes_back_to_the_lease_above_the_tables_epoch() {
         let store = ScriptedStore::new();
         let docs = pipeline_docs();
         let f = TestFactory::new(&["a"]);
@@ -1050,13 +1052,51 @@ mod tests {
         until(&mut n.status, "running", is(Actual::Running)).await;
         f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
         eventually("the first row lands", || f.ids("a") == [1]).await;
-        f.sink.take_over(&t("a"), 2);
+        f.sink.take_over(&t("a"), 5);
         f.source.commit(FakeTxn::new().insert(&t("a"), row(2)));
-        until(&mut n.status, "running under epoch 2", |s| {
-            s.actual == Some(Actual::Running) && s.owner_epoch == Some(2)
+        until(&mut n.status, "running under epoch 6", |s| {
+            s.actual == Some(Actual::Running) && s.owner_epoch == Some(6)
         })
         .await;
         eventually("the second row lands", || f.ids("a") == [1, 2]).await;
+        assert_eq!(f.builds(), 2, "one worker run per fence");
+    }
+
+    /// The table moves past the jumped epoch again: another lease store writes to the lake.
+    /// The pipeline goes to `ERROR` and keeps its lease, rather than climbing.
+    #[tokio::test(start_paused = true)]
+    async fn a_second_fence_past_a_jumped_epoch_is_an_error() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        let mut n = node("n1", &store, &docs, &f);
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
+        eventually("the first row lands", || f.ids("a") == [1]).await;
+        f.sink.take_over(&t("a"), 5);
+        f.source.commit(FakeTxn::new().insert(&t("a"), row(2)));
+        until(&mut n.status, "running under epoch 6", |s| {
+            s.owner_epoch == Some(6)
+        })
+        .await;
+        eventually("the second row lands", || f.ids("a") == [1, 2]).await;
+        f.sink.take_over(&t("a"), 9);
+        f.source.commit(FakeTxn::new().insert(&t("a"), row(3)));
+        until(&mut n.status, "ERROR", |s| {
+            matches!(s.actual, Some(Actual::Error { .. }))
+        })
+        .await;
+        let Some(Actual::Error { error, code }) = n.status.borrow().actual.clone() else {
+            unreachable!()
+        };
+        assert!(error.contains("another lease store"), "{error}");
+        assert_eq!(code, None);
+        assert_eq!(n.status.borrow().owner_epoch, Some(6), "the lease is kept");
+        tokio::time::sleep(Duration::from_secs(60)).await;
+        assert_eq!(f.builds(), 2, "no climb");
+        assert_eq!(f.ids("a"), [1, 2]);
+        let lease = leases(&store).observe("p").await.unwrap().unwrap();
+        assert_eq!((lease.doc.owner_epoch, lease.doc.released), (6, false));
     }
 
     /// A worker that failed transiently, then a start that fails for good: `ERROR` holds, and
```

Change `crates/data-avalanche-state/src/lease.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lease.rs b/crates/data-avalanche-state/src/lease.rs
index 4c316d8..97ba66c 100644
--- a/crates/data-avalanche-state/src/lease.rs
+++ b/crates/data-avalanche-state/src/lease.rs
@@ -576,6 +576,23 @@ mod tests {
         );
     }
 
+    #[tokio::test]
+    async fn take_over_at_jumps_to_the_floor_and_never_below_the_next_epoch() {
+        let l = leases();
+        mine(&l, "orders", "a").await;
+        let observed = l.observe("orders").await.unwrap().unwrap();
+        let b = l
+            .take_over_at(&observed, &node("b"), DUR, t(20), 6)
+            .await
+            .unwrap();
+        assert_eq!(b.doc.owner_epoch, 6);
+        let c = l.take_over_at(&b, &node("c"), DUR, t(21), 3).await.unwrap();
+        assert_eq!(
+            c.doc.owner_epoch, 7,
+            "a floor below the next epoch changes nothing"
+        );
+    }
+
     #[tokio::test]
     async fn take_over_refuses_to_overflow_the_epoch() {
         let l = leases();
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-state --lib lease::`

Expected: FAIL to compile: `take_over_at` doesn't exist.

```text
error[E0599]: no method named `take_over_at` found for struct `lease::Leases` in the current scope
error: could not compile `data-avalanche-state` (lib test) due to 2 previous errors
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/claim.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/claim.rs b/crates/data-avalanche-cluster/src/claim.rs
index 8020426..fe0983c 100644
--- a/crates/data-avalanche-cluster/src/claim.rs
+++ b/crates/data-avalanche-cluster/src/claim.rs
@@ -15,10 +15,11 @@ use crate::LeaseTiming;
 #[derive(Debug)]
 pub enum Claimed {
     /// This node holds the lease; `written` is when its write was sent (the renewer's first
-    /// `last_renew_local`).
+    /// `last_renew_local`). `jumped`: the floor ([`Claim::at_least`]) set its epoch.
     Mine {
         lease: Versioned<LeaseDoc>,
         written: Instant,
+        jumped: bool,
     },
     /// Another holder has it. Unless it renews, a step at or after `takeover_at` takes it over.
     HeldBy {
@@ -36,6 +37,8 @@ pub struct Claim {
     timing: LeaseTiming,
     /// The live holder's lease version this node watches, and when it first read it.
     watched: Option<(ObjectVersion, Instant)>,
+    /// The least epoch this process's own lease is taken over at (DA-CLUS-14); 0 for none.
+    floor: u64,
 }
 
 impl Claim {
@@ -46,9 +49,17 @@ impl Claim {
             holder,
             timing,
             watched: None,
+            floor: 0,
         }
     }
 
+    /// The next takeover of this process's own lease runs at an epoch of at least `epoch`: a
+    /// table was written at `epoch - 1`, later than the lease knew (DA-CLUS-14). Another
+    /// holder's lease is claimed as ever, and the floor is dropped.
+    pub fn at_least(&mut self, epoch: u64) {
+        self.floor = epoch;
+    }
+
     /// One attempt: acquire a free lease, or watch a live one and take it over once it went a
     /// full `lease_duration` without changing.
     ///
@@ -68,9 +79,11 @@ impl Claim {
         {
             Acquired::Mine(lease) => {
                 self.watched = None;
+                self.floor = 0;
                 return Ok(Claimed::Mine {
                     lease,
                     written: sent,
+                    jumped: false,
                 });
             }
             Acquired::HeldBy(seen) => seen,
@@ -81,6 +94,7 @@ impl Claim {
         // epoch with a worker that already ran. This rests on (node id, incarnation) naming one
         // process: the incarnation is its start in milliseconds, so a restarted process waits.
         if !seen.doc.is_held_by(&self.holder) {
+            self.floor = 0;
             // The wait runs from when the version was read, never from before: a renewal that
             // landed while the read was in flight must count.
             let read = Instant::now();
@@ -99,17 +113,23 @@ impl Claim {
                 });
             }
         }
+        // The floor holds until a takeover lands: a failed write tries it again.
+        let floor = self.floor;
+        let proposed = seen.doc.owner_epoch.checked_add(1).map(|e| e.max(floor));
         let sent = Instant::now();
         match self
             .leases
-            .take_over(&seen, &self.holder, self.timing.duration, Utc::now())
+            .take_over_at(&seen, &self.holder, self.timing.duration, Utc::now(), floor)
             .await
         {
             Ok(lease) => {
                 self.watched = None;
+                self.floor = 0;
+                let jumped = Some(lease.doc.owner_epoch) != seen.doc.owner_epoch.checked_add(1);
                 Ok(Claimed::Mine {
                     lease,
                     written: sent,
+                    jumped,
                 })
             }
             // The holder renewed, or another challenger won: watch again.
@@ -127,13 +147,16 @@ impl Claim {
                 match now {
                     Some(lease)
                         if lease.doc.is_held_by(&self.holder)
-                            && Some(lease.doc.owner_epoch)
-                                == seen.doc.owner_epoch.checked_add(1) =>
+                            && Some(lease.doc.owner_epoch) == proposed =>
                     {
                         self.watched = None;
+                        self.floor = 0;
+                        let jumped =
+                            Some(lease.doc.owner_epoch) != seen.doc.owner_epoch.checked_add(1);
                         Ok(Claimed::Mine {
                             lease,
                             written: sent,
+                            jumped,
                         })
                     }
                     _ => Err(e),
```

Change `crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index d293d37..d57ec31 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -144,6 +144,9 @@ pub struct PipelineController {
 enum Left {
     /// The lease is gone: claim it again.
     Lost,
+    /// A table was written at `current_epoch`, past this lease's epoch: claim again, and take
+    /// this process's own lease over above it (DA-CLUS-14).
+    Fenced { current_epoch: u64 },
     /// The target became "don't own": the lease is released.
     Released,
     /// The target channel closed: the process is shutting down.
@@ -256,7 +259,11 @@ impl PipelineController {
                 continue;
             }
             let wait = match claim.step().await {
-                Ok(Claimed::Mine { lease, written }) => {
+                Ok(Claimed::Mine {
+                    lease,
+                    written,
+                    jumped,
+                }) => {
                     tracing::info!(
                         pipeline = %self.pipeline.name,
                         owner_epoch = lease.doc.owner_epoch,
@@ -270,8 +277,13 @@ impl PipelineController {
                         written,
                         Arc::clone(&self.metrics),
                     );
-                    match self.owned(run, &mut target).await {
+                    match self.owned(run, jumped, &mut target).await {
                         Left::Shutdown => return,
+                        Left::Fenced { current_epoch } => {
+                            claim.at_least(current_epoch.saturating_add(1));
+                            self.standby(None);
+                            continue;
+                        }
                         // Not this node's until a step says otherwise, though claiming fails.
                         Left::Lost => {
                             self.standby(None);
@@ -300,14 +312,25 @@ impl PipelineController {
     }
 
     /// Holds the lease and reconciles the worker until the lease is lost or let go.
-    async fn owned(&self, run: LeaseRun, target: &mut watch::Receiver<Target>) -> Left {
+    /// `jumped`: the term's epoch was raised past the lease's to clear a table's (DA-CLUS-14).
+    async fn owned(
+        &self,
+        run: LeaseRun,
+        jumped: bool,
+        target: &mut watch::Receiver<Target>,
+    ) -> Left {
         self.node.owned_by(1);
-        let left = self.reconcile(run, target).await;
+        let left = self.reconcile(run, jumped, target).await;
         self.node.owned_by(-1);
         left
     }
 
-    async fn reconcile(&self, run: LeaseRun, target: &mut watch::Receiver<Target>) -> Left {
+    async fn reconcile(
+        &self,
+        run: LeaseRun,
+        jumped: bool,
+        target: &mut watch::Receiver<Target>,
+    ) -> Left {
         let mut worker: Option<Running> = None;
         let mut error: Option<Failure> = None;
         let mut dumped = false;
@@ -457,14 +480,41 @@ impl PipelineController {
                                 );
                                 retry = Some((at, e.to_string()));
                             }
-                            Recovery::Lease => {
-                                tracing::error!(
-                                    pipeline = %self.pipeline.name,
-                                    "worker stopped for the lease: {e}"
-                                );
-                                let _ = run.stop().await;
-                                return Left::Lost;
-                            }
+                            Recovery::Lease => match e {
+                                // DA-CLUS-14: the term already jumped past the table's epoch,
+                                // and the table moved past it again. Another lease store
+                                // writes to this lake; climbing on would defeat the fence.
+                                WorkerError::Fenced {
+                                    table,
+                                    current_epoch,
+                                } if jumped => {
+                                    let why = format!(
+                                        "{table} was written at owner epoch {current_epoch}, past this lease's {}, which had already been raised past the table's: another lease store (another globals.state) writes to this lake. Let one cluster write it, then stop and resume the pipeline",
+                                        run.owner_epoch
+                                    );
+                                    tracing::error!(pipeline = %self.pipeline.name, "pipeline in ERROR: {why}");
+                                    error = Some(Failure {
+                                        error: why,
+                                        code: None,
+                                    });
+                                }
+                                WorkerError::Fenced { current_epoch, .. } => {
+                                    tracing::error!(
+                                        pipeline = %self.pipeline.name,
+                                        "worker stopped for the lease: {e}"
+                                    );
+                                    let _ = run.stop().await;
+                                    return Left::Fenced { current_epoch };
+                                }
+                                e => {
+                                    tracing::error!(
+                                        pipeline = %self.pipeline.name,
+                                        "worker stopped for the lease: {e}"
+                                    );
+                                    let _ = run.stop().await;
+                                    return Left::Lost;
+                                }
+                            },
                             Recovery::Error => {
                                 let code = e.code().map(str::to_owned);
                                 tracing::error!(
```

Change `crates/data-avalanche-state/src/lease.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lease.rs b/crates/data-avalanche-state/src/lease.rs
index 97ba66c..c4a8ee9 100644
--- a/crates/data-avalanche-state/src/lease.rs
+++ b/crates/data-avalanche-state/src/lease.rs
@@ -234,6 +234,24 @@ impl Leases {
         holder: &Holder,
         lease_duration: Duration,
         now: DateTime<Utc>,
+    ) -> Result<Versioned<LeaseDoc>, StateError> {
+        self.take_over_at(observed, holder, lease_duration, now, 0)
+            .await
+    }
+
+    /// [`Self::take_over`] under an epoch of at least `at_least`: the lake has seen a later
+    /// epoch than the lease names (DA-CLUS-14), and a term must never share an epoch with a
+    /// writer that already ran.
+    ///
+    /// # Errors
+    /// As [`Self::take_over`].
+    pub async fn take_over_at(
+        &self,
+        observed: &Versioned<LeaseDoc>,
+        holder: &Holder,
+        lease_duration: Duration,
+        now: DateTime<Utc>,
+        at_least: u64,
     ) -> Result<Versioned<LeaseDoc>, StateError> {
         let Some(epoch) = observed.doc.owner_epoch.checked_add(1) else {
             return Err(StateError::Corrupt {
@@ -242,6 +260,7 @@ impl Leases {
                 reason: "owner_epoch is at its maximum".to_owned(),
             });
         };
+        let epoch = epoch.max(at_least);
         let doc = Self::new_doc(
             &observed.doc.pipeline_id,
             holder,
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-state --lib lease::`

Expected: PASS, 16 tests.

```text
test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 50 filtered out; finished in 0.00s
```

Run: `cargo test -p data-avalanche-cluster --lib -- claim:: controller::`

Expected: PASS, 31 tests.

```text
test result: ok. 31 passed; 0 failed; 0 ignored; 0 measured; 43 filtered out; finished in 0.21s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1127 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/claim.rs \
  crates/data-avalanche-cluster/src/controller.rs \
  crates/data-avalanche-state/src/lease.rs
git commit -m "fix(cluster): a lake ahead of the lease — the own lease is taken over above the table's epoch once, and a second fence past the jump is an ERROR naming another lease store (DA-CLUS-14)"
```

---

### Task 9: Discovery, the advertised address and the join (`data-avalanche-cluster`)

**Files:**
- Modify: `Cargo.lock`
- Modify: `Cargo.toml`
- Modify: `crates/data-avalanche-cluster/Cargo.toml`
- Create: `crates/data-avalanche-cluster/src/discovery.rs`
- Create: `crates/data-avalanche-cluster/src/error.rs`
- Modify: `crates/data-avalanche-cluster/src/gossip.rs`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`
- Modify: `crates/data-avalanche-cluster/src/node.rs`
- Modify: `crates/data-avalanche-cluster/src/reconciler.rs`
- Modify: `crates/data-avalanche/src/run.rs`

**Interfaces:**
- Consumes: `Announcer`, `Members`, `Gossip::set_phase` (Task 4); `data_avalanche_config::{Cluster, Discovery, Memberlist, HostPort}`.
- Produces:
  - `pub enum Seeds { Static(Vec<HostPort>), Dns { name: String, port: u16 } }`; `Seeds::new(&Cluster)`, `async Seeds::resolve(&self, me: &[SocketAddr]) -> Vec<SocketAddr>`
  - `discover(seeds: Seeds, me: Vec<SocketAddr>, announcer: Announcer, members: watch::Receiver<Members>) -> JoinHandle<()>`; `pub const REDISCOVER: Duration` (5 s)
  - `async advertise(m: &Memberlist, interfaces: &[IpAddr]) -> Result<SocketAddr, ClusterError>`; `interfaces() -> Vec<IpAddr>`
  - `node::join(gossip: &Gossip, seeded: bool)` (async); `pub const JOIN_WAIT: Duration` (5 s)
  - `pub enum ClusterError { NodeName(String), AdvertiseAddr(String), Bind { addr: SocketAddr, source: io::Error } }` (`error.rs`, re-exported); `node_identity` and `advertise` return it

D-M8a.8, D-M8a.15…17. Clustering §10.2 (D-4): the seeds are the `join_members` (`static`) or the addresses of `discovery_dns_name` at `advertise_port` (`dns`, a headless Service). The node's own addresses are left out. `discover` asks them at start and then every 5 s while the node has no peer, so the first pod of a StatefulSet, and a node cut off by a partition, find the others again. The advertised address must be one the others can reach: foca replies to the address in the sender's identity. The order is `advertise_addr`, else a specific `bind_addr`, else the host's first private IPv4 (`if-addrs`), else an error naming the key. A seeded node stays `Joining` until it hears a peer, at most 5 s. Turning Active alone would give it an empty ring that it fills with itself (Task 5), and claims that flap once the peers arrive. The string errors of M6b's `node_identity` become `ClusterError`.

- [ ] **Step 1: Write the failing tests**

Create `crates/data-avalanche-cluster/src/discovery.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/discovery.rs b/crates/data-avalanche-cluster/src/discovery.rs
new file mode 100644
index 0000000..54d6f87
--- /dev/null
+++ b/crates/data-avalanche-cluster/src/discovery.rs
@@ -0,0 +1,135 @@
+#[cfg(test)]
+mod tests {
+    use std::net::Ipv4Addr;
+
+    use super::*;
+    use crate::test_support::{config, eventually};
+
+    fn memberlist() -> Memberlist {
+        config(&["p"]).globals.cluster.memberlist
+    }
+
+    fn ip(s: &str) -> IpAddr {
+        s.parse().unwrap()
+    }
+
+    #[tokio::test]
+    async fn the_advertised_address_is_the_configured_one_else_a_bind_address_else_a_private_one() {
+        let ifs = [
+            ip("127.0.0.1"),
+            ip("203.0.113.9"),
+            ip("10.1.2.3"),
+            ip("192.168.0.4"),
+        ];
+        let mut m = memberlist();
+        m.advertise_port = 7000;
+        assert_eq!(m.bind_addr, [IpAddr::V4(Ipv4Addr::UNSPECIFIED)]);
+        assert_eq!(
+            advertise(&m, &ifs).await.unwrap(),
+            "10.1.2.3:7000".parse().unwrap()
+        );
+        m.bind_addr = vec![ip("0.0.0.0"), ip("172.16.0.8")];
+        assert_eq!(
+            advertise(&m, &ifs).await.unwrap(),
+            "172.16.0.8:7000".parse().unwrap()
+        );
+        m.advertise_addr = "10.9.9.9".into();
+        assert_eq!(
+            advertise(&m, &ifs).await.unwrap(),
+            "10.9.9.9:7000".parse().unwrap()
+        );
+        m.advertise_addr = "localhost".into();
+        assert!(advertise(&m, &ifs).await.unwrap().ip().is_loopback());
+    }
+
+    #[tokio::test]
+    async fn no_private_address_and_no_setting_is_an_error_naming_the_key() {
+        let m = memberlist();
+        let e = advertise(&m, &[ip("127.0.0.1"), ip("203.0.113.9")])
+            .await
+            .unwrap_err();
+        assert!(e.to_string().contains("advertise_addr"), "{e}");
+        let mut m = memberlist();
+        m.advertise_addr = "no-such-host.invalid".into();
+        let e = advertise(&m, &[]).await.unwrap_err();
+        assert!(e.to_string().contains("no-such-host.invalid"), "{e}");
+    }
+
+    #[tokio::test]
+    async fn static_seeds_resolve_and_leave_this_node_out() {
+        let seeds = Seeds::Static(vec![
+            "127.0.0.1:7001".parse().unwrap(),
+            "127.0.0.1:7002".parse().unwrap(),
+            "no-such-host.invalid:7003".parse().unwrap(),
+        ]);
+        let me = ["127.0.0.1:7002".parse().unwrap()];
+        assert_eq!(
+            seeds.resolve(&me).await,
+            ["127.0.0.1:7001".parse().unwrap()]
+        );
+    }
+
+    #[tokio::test]
+    async fn dns_seeds_are_every_address_at_the_advertise_port() {
+        let mut c = config(&["p"]).globals.cluster;
+        c.discovery = Discovery::Dns;
+        c.discovery_dns_name = Some("localhost".into());
+        c.memberlist.advertise_port = 7010;
+        let seeds = Seeds::new(&c);
+        assert_eq!(
+            seeds,
+            Seeds::Dns {
+                name: "localhost".into(),
+                port: 7010
+            }
+        );
+        let found = seeds.resolve(&[]).await;
+        assert!(!found.is_empty());
+        assert!(
+            found
+                .iter()
+                .all(|a| a.ip().is_loopback() && a.port() == 7010),
+            "{found:?}"
+        );
+    }
+
+    /// A node that started before its seed asks again while alone, and joins once the seed is
+    /// up.
+    #[tokio::test(start_paused = true)]
+    async fn a_node_without_peers_asks_its_seeds_again() {
+        use crate::test_support::{Hub, at, gossip_settings};
+        let hub = Hub::new();
+        let start = |name: &str, n: u8| {
+            let (port, inbox) = hub.port(at(n));
+            let me = crate::NodeIdentity {
+                node_id: name.into(),
+                incarnation: 1,
+                rev: 0,
+                addr: at(n),
+                meta: crate::NodeMeta {
+                    phase: crate::Phase::Active,
+                    config_generation: 1,
+                    build: "1.0.0".into(),
+                },
+            };
+            let key = crate::GossipKey::derive(&[7; 32]);
+            crate::Gossip::start(me, key, gossip_settings(), std::sync::Arc::new(port), inbox)
+        };
+        let b = start("b", 2);
+        let seeds = Seeds::Static(vec![HostPort {
+            host: at(1).ip().to_string(),
+            port: at(1).port(),
+        }]);
+        let _discovery = discover(seeds, vec![at(2)], b.announcer(), b.members());
+        tokio::time::sleep(Duration::from_secs(12)).await;
+        let a = start("a", 1);
+        let t = tokio::time::Instant::now();
+        eventually("b joins a", || a.members().borrow().peers() == 1).await;
+        assert!(
+            t.elapsed() <= REDISCOVER + Duration::from_secs(1),
+            "{:?}",
+            t.elapsed()
+        );
+        drop(b);
+    }
+}
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index da781a5..1b96c9e 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -5,6 +5,8 @@
 
 pub mod claim;
 pub mod controller;
+pub mod discovery;
+mod error;
 pub mod gossip;
 pub mod lease;
 pub mod node;
```

Change `crates/data-avalanche-cluster/src/node.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/node.rs b/crates/data-avalanche-cluster/src/node.rs
index 7ce0c65..f668b66 100644
--- a/crates/data-avalanche-cluster/src/node.rs
+++ b/crates/data-avalanche-cluster/src/node.rs
@@ -30,7 +30,59 @@ mod tests {
 
     use super::*;
     use crate::Ring;
-    use crate::test_support::{STABLE, TestCluster, at, eventually, leases, settles, timing};
+    use crate::test_support::{
+        Hub, STABLE, TestCluster, at, eventually, gossip_settings, leases, settles, timing,
+    };
+
+    fn joining(hub: &std::sync::Arc<Hub>, name: &str, n: u8) -> Gossip {
+        let (port, inbox) = hub.port(at(n));
+        let me = crate::NodeIdentity {
+            node_id: name.into(),
+            incarnation: 1,
+            rev: 0,
+            addr: at(n),
+            meta: crate::NodeMeta {
+                phase: Phase::Joining,
+                config_generation: 1,
+                build: "1.0.0".into(),
+            },
+        };
+        let key = crate::GossipKey::derive(&[7; 32]);
+        Gossip::start(me, key, gossip_settings(), std::sync::Arc::new(port), inbox)
+    }
+
+    fn phase(g: &Gossip, of: &str) -> Option<Phase> {
+        g.members().borrow().nodes.get(of).map(|m| m.meta.phase)
+    }
+
+    #[tokio::test(start_paused = true)]
+    async fn a_node_without_seeds_is_active_at_once() {
+        let hub = Hub::new();
+        let a = joining(&hub, "a", 1);
+        let t = Instant::now();
+        join(&a, false).await;
+        assert_eq!(t.elapsed(), Duration::ZERO);
+        eventually("a active", || phase(&a, "a") == Some(Phase::Active)).await;
+    }
+
+    /// A seed that answers: active once a peer is known; one that doesn't: alone after the wait.
+    #[tokio::test(start_paused = true)]
+    async fn a_node_with_seeds_waits_for_a_peer_then_is_active() {
+        let hub = Hub::new();
+        let a = joining(&hub, "a", 1);
+        join(&a, false).await;
+        let b = joining(&hub, "b", 2);
+        b.announce(vec![at(1)]);
+        join(&b, true).await;
+        assert_eq!(b.members().borrow().peers(), 1);
+        eventually("b active on a", || phase(&a, "b") == Some(Phase::Active)).await;
+        let c = joining(&hub, "c", 3);
+        c.announce(vec![at(9)]);
+        let t = Instant::now();
+        join(&c, true).await;
+        assert_eq!(t.elapsed(), JOIN_WAIT);
+        assert_eq!(c.members().borrow().peers(), 0);
+    }
 
     const IDS: [&str; 4] = ["orders", "events", "customers", "users"];
 
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib -- discovery:: node:: wire::`

Expected: FAIL to compile: the `discovery` module has only its tests, and `node::join` doesn't exist.

```text
error[E0583]: file not found for module `error`
error[E0425]: cannot find type `Memberlist` in this scope
error[E0425]: cannot find type `IpAddr` in this scope  --> crates/data-avalanche-cluster/src/discovery.rs:12:23
error[E0433]: cannot find type `IpAddr` in this scope  --> crates/data-avalanche-cluster/src/discovery.rs:26:34
error[E0422]: cannot find struct, variant or union type `HostPort` in this scope
error[E0433]: cannot find type `Duration` in this scope
error[E0425]: cannot find value `REDISCOVER` in this scope
error[E0425]: cannot find value `JOIN_WAIT` in this scope  --> crates/data-avalanche-cluster/src/node.rs:83:33
error[E0425]: cannot find function `advertise` in this scope  --> crates/data-avalanche-cluster/src/discovery.rs:28:13
error[E0425]: cannot find function `advertise` in this scope  --> crates/data-avalanche-cluster/src/discovery.rs:33:13
```

- [ ] **Step 3: Implement**

Change `Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index 7b86326..e4a54c1 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1736,9 +1736,11 @@ dependencies = [
  "foca",
  "gethostname",
  "getrandom 0.3.4",
+ "if-addrs",
  "object_store 0.14.2",
  "rand 0.10.3",
  "serde",
+ "thiserror 2.0.21",
  "tokio",
  "tracing",
  "xxhash-rust",
@@ -3590,6 +3592,16 @@ dependencies = [
  "icu_properties",
 ]
 
+[[package]]
+name = "if-addrs"
+version = "0.15.0"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "c0a05c691e1fae256cf7013d99dad472dc52d5543322761f83ec8d47eab40d2b"
+dependencies = [
+ "libc",
+ "windows-sys 0.61.2",
+]
+
 [[package]]
 name = "indexmap"
 version = "2.14.2"
```

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
index b21fe5b..92056bf 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -80,8 +80,10 @@ gethostname = "1.1"
 clap = { version = "4.6.7", features = ["derive"] }
 axum = { version = "0.8.9", default-features = false, features = ["http1", "tokio"] }
 # The cluster (M8a, roadmap §2.1): the ring's hash (the anchor's `hash_algo: xxh3`); the SWIM
-# core the gossip drives (Q-M8-1, probe `prototypes/cluster-capability`) and its random source.
+# core the gossip drives (Q-M8-1, probe `prototypes/cluster-capability`) and its random source;
+# the host's addresses, for an unset `advertise_addr`.
 xxhash-rust = { version = "0.8", features = ["xxh3"] }
+if-addrs = "0.15"
 foca = { version = "=2.0.0", default-features = false, features = ["std", "serde", "postcard-codec"] }
 rand = { version = "0.10", default-features = false, features = ["std", "std_rng", "sys_rng"] }
 
```

Change `crates/data-avalanche-cluster/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-cluster/Cargo.toml b/crates/data-avalanche-cluster/Cargo.toml
index 2aa8055..2b73f41 100644
--- a/crates/data-avalanche-cluster/Cargo.toml
+++ b/crates/data-avalanche-cluster/Cargo.toml
@@ -22,8 +22,10 @@ data-avalanche-telemetry.workspace = true
 foca.workspace = true
 gethostname.workspace = true
 getrandom.workspace = true
+if-addrs.workspace = true
 rand.workspace = true
 serde.workspace = true
+thiserror.workspace = true
 tokio = { workspace = true, features = ["net", "sync"] }
 tracing.workspace = true
 xxhash-rust.workspace = true
```

Change `crates/data-avalanche-cluster/src/discovery.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/discovery.rs b/crates/data-avalanche-cluster/src/discovery.rs
index 54d6f87..91279cc 100644
--- a/crates/data-avalanche-cluster/src/discovery.rs
+++ b/crates/data-avalanche-cluster/src/discovery.rs
@@ -1,3 +1,130 @@
+//! How a node finds its peers (Clustering §3.5, §10.2, D-4) and the address it gives them.
+//! `static` asks `memberlist.join_members`; `dns` asks every address `discovery_dns_name`
+//! resolves to (a Kubernetes headless Service), at `advertise_port`. Seeds are asked at start and
+//! again every [`REDISCOVER`] while the node has no peer, so a node that lost them all, or
+//! started before them, finds its way back; once joined, SWIM carries the membership.
+
+use std::net::{IpAddr, SocketAddr};
+use std::time::Duration;
+
+use data_avalanche_config::{Cluster, Discovery, HostPort, Memberlist};
+use tokio::sync::watch;
+use tokio::task::JoinHandle;
+
+use crate::gossip::Announcer;
+use crate::{ClusterError, Members};
+
+/// How often a node without peers asks its seeds again.
+pub const REDISCOVER: Duration = Duration::from_secs(5);
+
+/// Where a node's seeds come from.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub enum Seeds {
+    Static(Vec<HostPort>),
+    Dns { name: String, port: u16 },
+}
+
+impl Seeds {
+    pub fn new(cluster: &Cluster) -> Self {
+        let m = &cluster.memberlist;
+        match (&cluster.discovery, &cluster.discovery_dns_name) {
+            (Discovery::Dns, Some(name)) => Self::Dns {
+                name: name.clone(),
+                port: m.advertise_port,
+            },
+            // V-096 refuses `dns` without a name.
+            _ => Self::Static(m.join_members.clone()),
+        }
+    }
+
+    /// The seeds' addresses now, without `me`'s own. A name that doesn't resolve is logged
+    /// and skipped: the next round asks again.
+    pub async fn resolve(&self, me: &[SocketAddr]) -> Vec<SocketAddr> {
+        let names: Vec<(String, u16)> = match self {
+            Self::Static(seeds) => seeds.iter().map(|s| (s.host.clone(), s.port)).collect(),
+            Self::Dns { name, port } => vec![(name.clone(), *port)],
+        };
+        let mut addrs = Vec::new();
+        for (host, port) in names {
+            match tokio::net::lookup_host((host.as_str(), port)).await {
+                Ok(found) => addrs.extend(found),
+                Err(e) => tracing::warn!("gossip seed {host}:{port} doesn't resolve: {e}"),
+            }
+        }
+        addrs.retain(|a| !me.contains(a));
+        addrs.sort_unstable();
+        addrs.dedup();
+        addrs
+    }
+}
+
+/// Asks the seeds at once, then every [`REDISCOVER`] while the node has no peer.
+pub fn discover(
+    seeds: Seeds,
+    me: Vec<SocketAddr>,
+    announcer: Announcer,
+    members: watch::Receiver<Members>,
+) -> JoinHandle<()> {
+    tokio::spawn(async move {
+        loop {
+            if members.borrow().peers() == 0 {
+                let addrs = seeds.resolve(&me).await;
+                if !addrs.is_empty() {
+                    tracing::debug!(?addrs, "asking the gossip seeds");
+                    announcer.announce(addrs);
+                }
+            }
+            tokio::time::sleep(REDISCOVER).await;
+        }
+    })
+}
+
+/// The address the others reach this node at: `advertise_addr` (an IP, or a name resolved
+/// now), else a specific `bind_addr`, else the host's first private IPv4 among `interfaces`;
+/// at `advertise_port`.
+///
+/// # Errors
+/// `advertise_addr` doesn't resolve, or no address qualifies.
+pub async fn advertise(m: &Memberlist, interfaces: &[IpAddr]) -> Result<SocketAddr, ClusterError> {
+    let port = m.advertise_port;
+    if !m.advertise_addr.is_empty() {
+        if let Ok(ip) = m.advertise_addr.parse::<IpAddr>() {
+            return Ok(SocketAddr::new(ip, port));
+        }
+        let found = tokio::net::lookup_host((m.advertise_addr.as_str(), port))
+            .await
+            .map_err(|e| {
+                ClusterError::AdvertiseAddr(format!("{:?} doesn't resolve: {e}", m.advertise_addr))
+            })?;
+        return found.into_iter().next().ok_or_else(|| {
+            ClusterError::AdvertiseAddr(format!("{:?} resolves to nothing", m.advertise_addr))
+        });
+    }
+    if let Some(ip) = m.bind_addr.iter().find(|ip| !ip.is_unspecified()) {
+        return Ok(SocketAddr::new(*ip, port));
+    }
+    interfaces
+        .iter()
+        .find(|ip| matches!(ip, IpAddr::V4(v4) if v4.is_private()))
+        .map(|ip| SocketAddr::new(*ip, port))
+        .ok_or_else(|| {
+            ClusterError::AdvertiseAddr(
+                "the gossip binds every address and the host has no private IPv4 address".into(),
+            )
+        })
+}
+
+/// The host's interface addresses, for [`advertise`].
+pub fn interfaces() -> Vec<IpAddr> {
+    match if_addrs::get_if_addrs() {
+        Ok(found) => found.into_iter().map(|i| i.ip()).collect(),
+        Err(e) => {
+            tracing::warn!("the host's addresses can't be listed: {e}");
+            Vec::new()
+        }
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use std::net::Ipv4Addr;
```

Create `crates/data-avalanche-cluster/src/error.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/error.rs b/crates/data-avalanche-cluster/src/error.rs
new file mode 100644
index 0000000..f1b4dae
--- /dev/null
+++ b/crates/data-avalanche-cluster/src/error.rs
@@ -0,0 +1,20 @@
+use std::net::SocketAddr;
+
+use thiserror::Error;
+
+/// Why a node can't take its place in the cluster.
+#[derive(Debug, Error)]
+pub enum ClusterError {
+    /// No usable node name (Clustering §3.4, D-1).
+    #[error("{0}: set globals.cluster.memberlist.node_name")]
+    NodeName(String),
+    /// No address the others could reach this node at.
+    #[error("{0}: set globals.cluster.memberlist.advertise_addr")]
+    AdvertiseAddr(String),
+    /// A gossip socket couldn't be bound.
+    #[error("the gossip can't bind {addr}: {source}")]
+    Bind {
+        addr: SocketAddr,
+        source: std::io::Error,
+    },
+}
```

Change `crates/data-avalanche-cluster/src/gossip.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/gossip.rs b/crates/data-avalanche-cluster/src/gossip.rs
index bddae95..7d4e73d 100644
--- a/crates/data-avalanche-cluster/src/gossip.rs
+++ b/crates/data-avalanche-cluster/src/gossip.rs
@@ -216,6 +216,17 @@ enum Command {
     Leave(oneshot::Sender<()>),
 }
 
+/// Sends a running gossip's announces; cheap to clone.
+#[derive(Debug, Clone)]
+pub struct Announcer(mpsc::UnboundedSender<Command>);
+
+impl Announcer {
+    /// Asks `seeds` to let this node in. A seed is known by its address only.
+    pub fn announce(&self, seeds: Vec<SocketAddr>) {
+        let _ = self.0.send(Command::Announce(seeds));
+    }
+}
+
 /// A running gossip. Dropping it stops the gossip without a leave, as a crash would.
 #[derive(Debug)]
 pub struct Gossip {
@@ -274,7 +285,11 @@ impl Gossip {
 
     /// Asks `seeds` to let this node in. A seed is known by its address only.
     pub fn announce(&self, seeds: Vec<SocketAddr>) {
-        let _ = self.commands.send(Command::Announce(seeds));
+        self.announcer().announce(seeds);
+    }
+
+    pub fn announcer(&self) -> Announcer {
+        Announcer(self.commands.clone())
     }
 
     /// Tells the others this node's new phase.
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 1b96c9e..ced2625 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -22,9 +22,13 @@ pub use controller::{
     Actual, ControllerDeps, ControllerSettings, ControllerStatus, Parts, PipelineController,
     Target, WorkerFactory,
 };
-pub use gossip::{Gossip, GossipSettings, Member, Members, NodeIdentity, NodeMeta, Phase};
+pub use discovery::{Seeds, advertise, discover, interfaces};
+pub use error::ClusterError;
+pub use gossip::{
+    Announcer, Gossip, GossipSettings, Member, Members, NodeIdentity, NodeMeta, Phase,
+};
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
-pub use node::leave;
+pub use node::{JOIN_WAIT, join, leave};
 pub use placement::Placement;
 pub use reconciler::{ClusterReconciler, ReconcilerSettings, node_identity};
 pub use ring::Ring;
```

Change `crates/data-avalanche-cluster/src/node.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/node.rs b/crates/data-avalanche-cluster/src/node.rs
index f668b66..b24d09c 100644
--- a/crates/data-avalanche-cluster/src/node.rs
+++ b/crates/data-avalanche-cluster/src/node.rs
@@ -1,7 +1,30 @@
-//! A node's way out of the cluster (Clustering §3.5, §8.3).
+//! A node's way into the cluster and out of it (Clustering §3.3, §3.5, §8.3).
+
+use std::time::Duration;
 
 use crate::{ClusterReconciler, Gossip, Phase};
 
+/// How long a starting node with seeds waits for its first peer before it goes on alone.
+pub const JOIN_WAIT: Duration = Duration::from_secs(5);
+
+/// Takes a node that started its gossip as `Joining` to `Active`. With seeds (`seeded`), it
+/// first waits, at most [`JOIN_WAIT`], to hear from a peer: a node that knows the cluster
+/// before its first ring takes the others' rings as they are and waits for its own place
+/// (`Placement`), where a node that knew no one would place everything on itself first.
+pub async fn join(gossip: &Gossip, seeded: bool) {
+    if seeded {
+        let mut members = gossip.members();
+        let peer = tokio::time::timeout(JOIN_WAIT, members.wait_for(|m| m.peers() > 0)).await;
+        match peer {
+            Ok(_) => tracing::info!("joined the gossip"),
+            Err(_) => tracing::info!(
+                "no gossip seed answered within {JOIN_WAIT:?}: going on alone, still asking"
+            ),
+        }
+    }
+    gossip.set_phase(Phase::Active);
+}
+
 /// Leaves the cluster on a shutdown. With peers, the node drains first (Clustering §8.3): its
 /// phase becomes `Draining`, which takes it off every ring at once, so each pipeline it runs is
 /// handed off (`max_concurrent_handoffs` at a time) and released, and its new owner acquires it
```

Change `crates/data-avalanche-cluster/src/reconciler.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/reconciler.rs b/crates/data-avalanche-cluster/src/reconciler.rs
index 8a0c2a9..8ec9cb8 100644
--- a/crates/data-avalanche-cluster/src/reconciler.rs
+++ b/crates/data-avalanche-cluster/src/reconciler.rs
@@ -12,7 +12,8 @@ use tokio::task::JoinHandle;
 use tokio::time::{Instant, MissedTickBehavior};
 
 use crate::{
-    Actual, ControllerDeps, ControllerStatus, Members, PipelineController, Placement, Target,
+    Actual, ClusterError, ControllerDeps, ControllerStatus, Members, PipelineController, Placement,
+    Target,
 };
 
 /// This node as a lease holder (Clustering §3.4, D-1): `node_id` is
@@ -21,19 +22,19 @@ use crate::{
 ///
 /// # Errors
 /// No `node_name` and no usable hostname.
-pub fn node_identity(cluster: &Cluster, started: SystemTime) -> Result<Holder, String> {
+pub fn node_identity(cluster: &Cluster, started: SystemTime) -> Result<Holder, ClusterError> {
     let node_id = match &cluster.memberlist.node_name {
         Some(name) => name.clone(),
-        None => gethostname::gethostname().into_string().map_err(|h| {
-            format!("the hostname {h:?} isn't UTF-8: set globals.cluster.memberlist.node_name")
-        })?,
+        None => gethostname::gethostname()
+            .into_string()
+            .map_err(|h| ClusterError::NodeName(format!("the hostname {h:?} isn't UTF-8")))?,
     };
     if node_id.is_empty() {
-        return Err("the hostname is empty: set globals.cluster.memberlist.node_name".into());
+        return Err(ClusterError::NodeName("the hostname is empty".into()));
     }
     let incarnation = started
         .duration_since(UNIX_EPOCH)
-        .map_err(|e| format!("the clock is before 1970: {e}"))?
+        .map_err(|e| ClusterError::NodeName(format!("the clock is before 1970: {e}")))?
         .as_millis();
     Ok(Holder {
         node_id,
```

Change `crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 98f1677..f6a0455 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -122,7 +122,8 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         })
         .into_future(),
     );
-    let holder = node_identity(&first.config.globals.cluster, started).map_err(Fatal::Config)?;
+    let holder = node_identity(&first.config.globals.cluster, started)
+        .map_err(|e| Fatal::Config(e.to_string()))?;
     let buckets = Buckets::new(&first.config.globals);
     let joined = join(&buckets, &holder.node_id, &text, &first.config).await?;
     let config = joined.config;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib -- discovery:: node:: wire::`

Expected: PASS, 12 tests.

```text
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 69 filtered out; finished in 0.47s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1134 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  Cargo.toml \
  crates/data-avalanche-cluster/Cargo.toml \
  crates/data-avalanche-cluster/src/discovery.rs \
  crates/data-avalanche-cluster/src/error.rs \
  crates/data-avalanche-cluster/src/gossip.rs \
  crates/data-avalanche-cluster/src/lib.rs \
  crates/data-avalanche-cluster/src/node.rs \
  crates/data-avalanche-cluster/src/reconciler.rs \
  crates/data-avalanche/src/run.rs
git commit -m "feat(cluster): discovery and the advertised address — static or dns seeds asked again while alone (D-4), the join's wait for a first peer, typed errors"
```

---

### Task 10: Three in-process nodes (`data-avalanche-cluster`, tests only)

**Files:**
- Create (test): `crates/data-avalanche-cluster/src/cluster_tests.rs`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`
- Modify: `crates/data-avalanche-cluster/src/node.rs`
- Modify (test): `crates/data-avalanche-cluster/src/test_support.rs`

**Interfaces:**
- Consumes: `TestCluster`, `TestNode`, `settles`, `runners` (Task 7); `node::{join, leave}` (Tasks 7, 9); `Ring` (Task 2).
- Produces: the scenarios in `cluster_tests.rs` (`#[cfg(test)]`).

D-M8a.21. The scenarios run whole nodes in one process on the paused clock: gossip on the hub, the reconciler, controllers and workers on the in-memory store.
1. Three nodes spread their pipelines as the ring says.
2. A dropped node's pipelines are taken over by the others once its leases expire, not before.
3. A partition {a, b} | {c} moves no pipeline while the leases are renewed: each side's ring changes, but the claims wait on leases their holders keep. The heal restores one ring.
4. A rolling restart (leave, then a new incarnation at a new address) keeps every pipeline running.

All along, `settles` checks that no pipeline is ever starting or running on two nodes. This is the safety property: the lease must give it, because the ring can't. The behaviour comes from Tasks 4–9, so the tests pass as they arrive. They are the cluster's regression suite on the paused clock, and Task 12 reruns the same scenarios against real processes.

- [ ] **Step 1: Write the tests**

Create `crates/data-avalanche-cluster/src/cluster_tests.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/cluster_tests.rs b/crates/data-avalanche-cluster/src/cluster_tests.rs
new file mode 100644
index 0000000..2559a96
--- /dev/null
+++ b/crates/data-avalanche-cluster/src/cluster_tests.rs
@@ -0,0 +1,158 @@
+//! Three in-process nodes (their gossip on a hub, their leases in one store, paused time): the
+//! cluster's scenarios, each checking at every step that no pipeline runs on two nodes, and
+//! that the rows committed meanwhile all land.
+
+use std::time::Duration;
+
+use tokio::time::Instant;
+
+use crate::test_support::{
+    STABLE, TestCluster, TestNode, at, eventually, runners, settles, timing,
+};
+use crate::{Ring, leave};
+
+const IDS: [&str; 6] = [
+    "orders",
+    "events",
+    "customers",
+    "users",
+    "payments",
+    "audit",
+];
+
+fn placed(nodes: &[&str]) -> impl Fn(&str) -> String {
+    let ring = Ring::new(nodes.iter().copied(), 128);
+    move |id| ring.owner(id).expect("a node").to_owned()
+}
+
+/// `a` first, then `b` and `c` through `a`; settled on the three-node ring.
+async fn three(c: &TestCluster) -> (TestNode, TestNode, TestNode) {
+    let a = c.start("a", 1, 1, None).await;
+    let b = c.start("b", 2, 1, Some(at(1))).await;
+    let n3 = c.start("c", 3, 1, Some(at(1))).await;
+    tokio::time::sleep(STABLE).await;
+    settles(
+        "spread over a, b and c",
+        &[&a, &b, &n3],
+        placed(&["a", "b", "c"]),
+    )
+    .await;
+    (a, b, n3)
+}
+
+/// For `d`, the pipelines stay where they run and none runs on two nodes.
+async fn steady(what: &str, nodes: &[&TestNode], d: Duration) {
+    let before = runners(nodes);
+    let t = Instant::now();
+    while t.elapsed() < d {
+        assert_eq!(
+            runners(nodes),
+            before,
+            "{what}: moved after {:?}",
+            t.elapsed()
+        );
+        tokio::time::sleep(Duration::from_millis(10)).await;
+    }
+}
+
+#[tokio::test(start_paused = true)]
+async fn three_nodes_spread_the_pipelines_and_each_runs_once() {
+    let c = TestCluster::new(&IDS);
+    let (a, b, n3) = three(&c).await;
+    let on = runners(&[&a, &b, &n3]);
+    for name in ["a", "b", "c"] {
+        assert!(
+            on.values().any(|n| n == &[name]),
+            "{name} runs nothing: {on:?}"
+        );
+    }
+    c.insert(1);
+    eventually("1 lands", || c.landed(&[1])).await;
+    for n in [&a, &b, &n3] {
+        let members = n.gossip.members().borrow().nodes.len();
+        assert_eq!(members, 3, "{}", n.name);
+    }
+}
+
+/// `c` crashes: the others see it gone after the suspicion, and take its pipelines once their
+/// leases went a lease_duration unrenewed. The rows committed meanwhile land once.
+#[tokio::test(start_paused = true)]
+async fn a_crashed_nodes_pipelines_are_taken_over_after_its_lease() {
+    let c = TestCluster::new(&IDS);
+    let (a, b, n3) = three(&c).await;
+    c.insert(1);
+    eventually("1 lands", || c.landed(&[1])).await;
+    let t = Instant::now();
+    drop(n3);
+    c.insert(2);
+    settles("on a and b", &[&a, &b], placed(&["a", "b"])).await;
+    let took = t.elapsed();
+    assert!(
+        took >= timing().duration,
+        "{took:?}: before the lease expired"
+    );
+    assert!(
+        took < timing().duration + Duration::from_secs(15),
+        "{took:?}"
+    );
+    eventually("2 lands", || c.landed(&[1, 2])).await;
+}
+
+/// `{a, b} | {c}`: each side thinks the other died, but every lease is still renewed, so no
+/// pipeline moves (the lease, not the ring, decides). After the heal and a stabilization the
+/// ring is whole again and the pipelines are where it puts them.
+#[tokio::test(start_paused = true)]
+async fn a_partition_moves_no_pipeline_while_the_leases_hold() {
+    let c = TestCluster::new(&IDS);
+    let (a, b, n3) = three(&c).await;
+    c.hub.cut(&[at(1), at(2)], &[at(3)]);
+    eventually("split", || {
+        a.gossip.members().borrow().peers() == 1 && n3.gossip.members().borrow().peers() == 0
+    })
+    .await;
+    c.insert(1);
+    steady("partitioned", &[&a, &b, &n3], Duration::from_secs(60)).await;
+    eventually("1 lands", || c.landed(&[1])).await;
+    c.hub.heal();
+    eventually("healed", || {
+        [&a, &b, &n3]
+            .iter()
+            .all(|n| n.gossip.members().borrow().peers() == 2)
+    })
+    .await;
+    tokio::time::sleep(STABLE).await;
+    settles("whole again", &[&a, &b, &n3], placed(&["a", "b", "c"])).await;
+    c.insert(2);
+    eventually("2 lands", || c.landed(&[1, 2])).await;
+}
+
+/// Every node leaves and comes back as its next incarnation, one at a time: each leave hands
+/// off without a lease wait, and the rows committed throughout land.
+#[tokio::test(start_paused = true)]
+async fn a_rolling_restart_keeps_every_pipeline_running_once() {
+    let c = TestCluster::new(&IDS);
+    let (a, b, n3) = three(&c).await;
+    let mut nodes = vec![a, b, n3];
+    let mut row = 0;
+    for i in 0..3 {
+        let gone = nodes.remove(0);
+        let (name, n) = (gone.name.clone(), u8::try_from(i + 1).unwrap());
+        let t = Instant::now();
+        let left = tokio::spawn(leave(gone.gossip, gone.reconciler));
+        let rest: Vec<&str> = nodes.iter().map(|n| n.name.as_str()).collect();
+        let refs: Vec<&TestNode> = nodes.iter().collect();
+        settles(&format!("{name} handed off"), &refs, placed(&rest)).await;
+        assert!(t.elapsed() < timing().duration, "{name}: {:?}", t.elapsed());
+        left.await.unwrap();
+        row += 1;
+        c.insert(row);
+        // Back as its next incarnation, seeded by a node that stayed.
+        let seed = if n == 1 { at(2) } else { at(1) };
+        nodes.push(c.start(&name, n, 2, Some(seed)).await);
+        tokio::time::sleep(STABLE).await;
+        let refs: Vec<&TestNode> = nodes.iter().collect();
+        settles(&format!("{name} back"), &refs, placed(&["a", "b", "c"])).await;
+    }
+    let all: Vec<i64> = (1..=row).collect();
+    eventually("every row lands", || c.landed(&all)).await;
+}
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index ced2625..1e65b61 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -4,6 +4,8 @@
 //! lease, and the reconciler that tells each controller whether to own.
 
 pub mod claim;
+#[cfg(test)]
+mod cluster_tests;
 pub mod controller;
 pub mod discovery;
 mod error;
```

Change `crates/data-avalanche-cluster/src/node.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/node.rs b/crates/data-avalanche-cluster/src/node.rs
index b24d09c..e118a64 100644
--- a/crates/data-avalanche-cluster/src/node.rs
+++ b/crates/data-avalanche-cluster/src/node.rs
@@ -114,9 +114,9 @@ mod tests {
     #[tokio::test(start_paused = true)]
     async fn a_leave_hands_every_pipeline_off_without_a_lease_wait() {
         let c = TestCluster::new(&IDS);
-        let a = c.start("a", 1, 1, None);
+        let a = c.start("a", 1, 1, None).await;
         settles("all on a", &[&a], |_| "a".into()).await;
-        let b = c.start("b", 2, 1, Some(at(1)));
+        let b = c.start("b", 2, 1, Some(at(1))).await;
         let two = Ring::new(["a", "b"], 128);
         tokio::time::sleep(STABLE).await;
         settles("spread over a and b", &[&a, &b], |id| {
@@ -148,14 +148,14 @@ mod tests {
     #[tokio::test(start_paused = true)]
     async fn a_lone_node_leaves_by_releasing_its_leases() {
         let c = TestCluster::new(&IDS);
-        let a = c.start("a", 1, 1, None);
+        let a = c.start("a", 1, 1, None).await;
         settles("all on a", &[&a], |_| "a".into()).await;
         leave(a.gossip, a.reconciler).await;
         for id in IDS {
             let lease = leases(&c.store).observe(id).await.unwrap().unwrap();
             assert!(lease.doc.released, "{id}");
         }
-        let a2 = c.start("a", 1, 2, None);
+        let a2 = c.start("a", 1, 2, None).await;
         let t = Instant::now();
         settles("all on the next a", &[&a2], |_| "a".into()).await;
         assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
```

Change `crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index baa8158..5cf2f4e 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -617,9 +617,15 @@ impl TestCluster {
         }
     }
 
-    /// Starts `name` at `at(n)` as its `incarnation`, active at once, announcing itself to
-    /// `seed`.
-    pub fn start(&self, name: &str, n: u8, incarnation: u64, seed: Option<SocketAddr>) -> TestNode {
+    /// Starts `name` at `at(n)` as its `incarnation`, as the binary does: its gossip joining,
+    /// announced to `seed`, then [`crate::join`], then its reconciler.
+    pub async fn start(
+        &self,
+        name: &str,
+        n: u8,
+        incarnation: u64,
+        seed: Option<SocketAddr>,
+    ) -> TestNode {
         let (port, inbox) = self.hub.port(at(n));
         let me = crate::NodeIdentity {
             node_id: name.to_owned(),
@@ -627,7 +633,7 @@ impl TestCluster {
             rev: 0,
             addr: at(n),
             meta: crate::NodeMeta {
-                phase: crate::Phase::Active,
+                phase: crate::Phase::Joining,
                 config_generation: 1,
                 build: "1.0.0".into(),
             },
@@ -637,6 +643,7 @@ impl TestCluster {
         if let Some(seed) = seed {
             gossip.announce(vec![seed]);
         }
+        crate::join(&gossip, seed.is_some()).await;
         let mut deps = deps(
             name,
             &self.store,
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p data-avalanche-cluster --lib cluster_tests::`

Expected: PASS, 4 tests: the behaviour is Tasks 4–9's.

```text
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 81 filtered out; finished in 2.62s
```

- [ ] **Step 3: Implement**

No code change.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib cluster_tests::`

Expected: PASS, 4 tests.

```text
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 81 filtered out; finished in 2.48s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1138 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/cluster_tests.rs \
  crates/data-avalanche-cluster/src/lib.rs \
  crates/data-avalanche-cluster/src/node.rs \
  crates/data-avalanche-cluster/src/test_support.rs
git commit -m "test(cluster): three in-process nodes — spread, a crash taken over after its lease, a partition that moves nothing, a rolling restart; no pipeline ever on two nodes"
```

---

### Task 11: The node in the cluster (`data-avalanche`)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/discovery.rs`
- Modify: `crates/data-avalanche-cluster/src/wire.rs`
- Modify: `crates/data-avalanche/src/run.rs`
- Modify: `crates/data-avalanche/src/startup.rs`
- Modify (test): `crates/data-avalanche/tests/e2e/harness.rs`

**Interfaces:**
- Consumes: `Udp`, `GossipKey` (Task 3); `Gossip`, `GossipSettings`, `NodeIdentity`, `NodeMeta`, `Phase` (Task 4); `ClusterReconciler`, `ReconcilerSettings` (Task 6); `leave` (Task 7); `Seeds`, `discover`, `advertise`, `interfaces`, `join` (Task 9).
- Produces:
  - `Udp::bind(addrs) -> Result<(Udp, mpsc::Receiver<Bytes>), ClusterError>`: a bind error names the address (`ClusterError::Bind`)
  - `Seeds::is_empty(&self) -> bool`: no seed configured
  - `startup::Joined { config, globals, fingerprint_key: [u8; 32], generation }` (the anchor's key, for the gossip)
  - `run`: the node binds UDP on `bind_addr:bind_port` (`Fatal::Startup` on an error), advertises its address (`Fatal::Config`), starts the gossip as `Joining` with the generation and `build = CARGO_PKG_VERSION`, starts discovery and the join, then runs the reconciler on the gossip's members. On SIGTERM it stops discovery and leaves (Task 7).
  - E2E harness: one `config.yaml` per test; `spawn(node, port, env)` sets `E2E_NODE`, `E2E_API_PORT`, `E2E_GOSSIP_PORT`, `E2E_ADVERTISE_PORT`; `free_udp_port()`

D-M8a.17, D-M8a.18. The binary now runs as a cluster node. A node runs the cluster's config generation (D-3), resolved with its own environment, so every per-node value of the E2E suite comes from `${E2E_*}` in one config. M6b's harness wrote one file per node, which the second node never read. Measured: it adopted the first node's generation and exited "Address already in use" on that node's API port. The `${…}` values inside the flow maps are quoted (a YAML 1.2 rule, M1 Review Focus 1). The suite's two-node lifecycle scenario now runs on real gossip, and the whole suite (33 scenarios) passes on either source.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-cluster/src/wire.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/wire.rs b/crates/data-avalanche-cluster/src/wire.rs
index 691fbeb..c7cfe30 100644
--- a/crates/data-avalanche-cluster/src/wire.rs
+++ b/crates/data-avalanche-cluster/src/wire.rs
@@ -176,6 +176,16 @@ mod tests {
         assert_eq!(format!("{ours:?}"), "GossipKey(..)");
     }
 
+    #[tokio::test]
+    async fn an_address_taken_is_an_error_naming_it() {
+        let (a, _in) = Udp::bind(&[SocketAddr::from(([127, 0, 0, 1], 0))])
+            .await
+            .unwrap();
+        let taken = a.local_addrs()[0];
+        let e = Udp::bind(&[taken]).await.unwrap_err();
+        assert!(e.to_string().contains(&taken.to_string()), "{e}");
+    }
+
     #[tokio::test]
     async fn packets_cross_udp_on_loopback() {
         let any = SocketAddr::from(([127, 0, 0, 1], 0));
```

Change `crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
index defa0fa..2329724 100644
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -46,6 +46,14 @@ fn free_port() -> u16 {
         .port()
 }
 
+/// A free UDP port on loopback: a node's gossip port.
+pub fn free_udp_port() -> u16 {
+    std::net::UdpSocket::bind("127.0.0.1:0")
+        .and_then(|s| s.local_addr())
+        .expect("a free UDP port")
+        .port()
+}
+
 /// What a test sets beyond the defaults (YAML flow entries, each starting with `, `).
 #[derive(Debug, Clone, Default)]
 pub struct Options<'a> {
@@ -89,7 +97,9 @@ pub struct E2e {
     /// The first node's (`n1`) API port.
     port: u16,
     dir: tempfile::TempDir,
-    /// The config with `@NODE@` and `@PORT@` to fill per node.
+    /// The config every node runs; its per-node values come from `E2E_*` variables, as a
+    /// cluster's one config takes them from each pod's environment (D-3: a node runs the
+    /// cluster's generation, not its own file).
     template: String,
     pub db: Db,
 }
@@ -155,22 +165,12 @@ impl E2e {
             db,
         };
         e.template = e.yaml(&o);
-        e.write_config("n1", e.port);
+        std::fs::write(e.config_path(), &e.template).expect("the config file");
         e
     }
 
-    fn config_path(&self, node: &str) -> PathBuf {
-        self.dir.path().join(format!("config-{node}.yaml"))
-    }
-
-    fn write_config(&self, node: &str, port: u16) -> PathBuf {
-        let path = self.config_path(node);
-        let text = self
-            .template
-            .replace("@NODE@", node)
-            .replace("@PORT@", &port.to_string());
-        std::fs::write(&path, text).expect("the config file");
-        path
+    fn config_path(&self) -> PathBuf {
+        self.dir.path().join("config.yaml")
     }
 
     /// The `source` block of a pipeline named `test` (on PostgreSQL the slot and publication
@@ -243,13 +243,13 @@ impl E2e {
 globals:
   cloud: aws
   state: {{ bucket: {bucket}, prefix: {prefix}/globals }}
-  api: {{ address: "127.0.0.1:@PORT@" }}
+  api: {{ address: "127.0.0.1:${{E2E_API_PORT}}" }}
   log: {{ level: info, format: compact }}
   cluster:
     lease_duration: 6s
     lease_renewal_interval: 2s
     drain_timeout: 30s
-    memberlist: {{ node_name: "@NODE@" }}
+    memberlist: {{ node_name: "${{E2E_NODE}}", bind_addr: [127.0.0.1], bind_port: "${{E2E_GOSSIP_PORT}}", advertise_addr: 127.0.0.1, advertise_port: "${{E2E_ADVERTISE_PORT}}" }}
 pipelines:
   {test}:
     id: {test}
@@ -271,17 +271,18 @@ pipelines:
 
     /// Starts the binary as node `n1`; `env` adds variables (a crash point).
     pub fn start(&self, env: &[(&str, &str)]) -> Proc {
-        self.spawn(&self.config_path("n1"), self.port, env)
+        self.spawn("n1", self.port, env)
     }
 
     /// Starts the binary as another node, with its own name and API port.
     pub fn start_node(&self, node: &str) -> Proc {
-        let port = free_port();
-        let path = self.write_config(node, port);
-        self.spawn(&path, port, &[])
+        self.spawn(node, free_port(), &[])
     }
 
-    fn spawn(&self, config: &std::path::Path, port: u16, env: &[(&str, &str)]) -> Proc {
+    /// Runs node `node` with its API on `port`, gossiping alone on a loopback port of its own;
+    /// `env` adds variables (and may set `E2E_ADVERTISE_PORT`).
+    fn spawn(&self, node: &str, port: u16, env: &[(&str, &str)]) -> Proc {
+        let gossip = free_udp_port().to_string();
         let log = self.dir.path().join(format!(
             "run-{}.log",
             std::time::SystemTime::now()
@@ -293,7 +294,7 @@ pipelines:
         let err = out.try_clone().expect("the log file");
         let mut cmd = Command::new(env!("CARGO_BIN_EXE_data-avalanche"));
         cmd.args(["run", "--config"])
-            .arg(config)
+            .arg(self.config_path())
             .env_clear()
             .env("PATH", std::env::var("PATH").unwrap_or_default())
             .env("AWS_ACCESS_KEY_ID", RUSTFS_ACCESS_KEY)
@@ -302,6 +303,10 @@ pipelines:
             .env("AWS_ENDPOINT_URL", RUSTFS_URL)
             .env("AWS_ALLOW_HTTP", "true")
             .env("RUST_BACKTRACE", "1")
+            .env("E2E_NODE", node)
+            .env("E2E_API_PORT", port.to_string())
+            .env("E2E_GOSSIP_PORT", &gossip)
+            .env("E2E_ADVERTISE_PORT", &gossip)
             .stdin(Stdio::null())
             .stdout(out)
             .stderr(err);
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib wire::`

Expected: FAIL: the bind error doesn't name the address yet.

```text
test wire::tests::an_address_taken_is_an_error_naming_it ... FAILED
thread 'wire::tests::an_address_taken_is_an_error_naming_it' (3561651) panicked at crates/data-avalanche-cluster/src/wire.rs:186:9:
test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 82 filtered out; finished in 0.00s
error: test failed, to rerun pass `-p data-avalanche-cluster --lib`
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/discovery.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/discovery.rs b/crates/data-avalanche-cluster/src/discovery.rs
index 91279cc..fac83fa 100644
--- a/crates/data-avalanche-cluster/src/discovery.rs
+++ b/crates/data-avalanche-cluster/src/discovery.rs
@@ -25,6 +25,11 @@ pub enum Seeds {
 }
 
 impl Seeds {
+    /// No seed configured: a node alone until another joins it.
+    pub fn is_empty(&self) -> bool {
+        matches!(self, Self::Static(seeds) if seeds.is_empty())
+    }
+
     pub fn new(cluster: &Cluster) -> Self {
         let m = &cluster.memberlist;
         match (&cluster.discovery, &cluster.discovery_dns_name) {
```

Change `crates/data-avalanche-cluster/src/wire.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/wire.rs b/crates/data-avalanche-cluster/src/wire.rs
index c7cfe30..e81c5d0 100644
--- a/crates/data-avalanche-cluster/src/wire.rs
+++ b/crates/data-avalanche-cluster/src/wire.rs
@@ -5,7 +5,6 @@
 //! tests carry them over a hub instead.
 
 use std::fmt;
-use std::io;
 use std::net::SocketAddr;
 use std::sync::Arc;
 
@@ -14,6 +13,8 @@ use tokio::net::UdpSocket;
 use tokio::sync::mpsc;
 use tokio::task::JoinHandle;
 
+use crate::ClusterError;
+
 /// The MAC's length: a full BLAKE3 output.
 pub const MAC_LEN: usize = 32;
 
@@ -83,13 +84,17 @@ impl Udp {
     ///
     /// # Errors
     /// An address can't be bound.
-    pub async fn bind(addrs: &[SocketAddr]) -> io::Result<(Self, mpsc::Receiver<Bytes>)> {
+    pub async fn bind(addrs: &[SocketAddr]) -> Result<(Self, mpsc::Receiver<Bytes>), ClusterError> {
         let (inbox, rx) = mpsc::channel(INBOX);
         let (mut sockets, mut readers) = (Vec::new(), Vec::new());
         for addr in addrs {
-            let socket = Arc::new(UdpSocket::bind(addr).await?);
+            let bind = |source| ClusterError::Bind {
+                addr: *addr,
+                source,
+            };
+            let socket = Arc::new(UdpSocket::bind(addr).await.map_err(bind)?);
             let (out, mut queued) = mpsc::channel::<(SocketAddr, Bytes)>(INBOX);
-            sockets.push((socket.local_addr()?, out));
+            sockets.push((socket.local_addr().map_err(bind)?, out));
             let s = Arc::clone(&socket);
             tokio::spawn(async move {
                 while let Some((to, packet)) = queued.recv().await {
```

Change `crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index f6a0455..c983a3e 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -1,13 +1,15 @@
 //! `data-avalanche run`.
 
 use std::future::IntoFuture;
+use std::net::SocketAddr;
 use std::sync::Arc;
 use std::sync::atomic::{AtomicBool, Ordering};
 use std::time::{Duration, SystemTime};
 
 use data_avalanche_cluster::{
-    ClusterReconciler, ControllerDeps, ControllerSettings, LeaseTiming, Member, Members, NodeMeta,
-    Phase, ReconcilerSettings, node_identity,
+    ClusterReconciler, ControllerDeps, ControllerSettings, Gossip, GossipKey, GossipSettings,
+    LeaseTiming, NodeIdentity, NodeMeta, Phase, ReconcilerSettings, Seeds, Udp, advertise,
+    discover, interfaces, join as join_gossip, leave, node_identity,
 };
 use data_avalanche_config::{
     Diagnostic, FingerprintKey, ProcessEnv, Report, ResolvedConfig, Severity, load_str,
@@ -16,7 +18,7 @@ use data_avalanche_pipeline::{Backoff, Tuning};
 use data_avalanche_state::Leases;
 use data_avalanche_telemetry::Metrics;
 use thiserror::Error;
-use tokio::sync::{Semaphore, oneshot, watch};
+use tokio::sync::{Semaphore, oneshot};
 
 use crate::http::{self, Http};
 use crate::startup::{Buckets, join};
@@ -151,31 +153,55 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
             usize::try_from(c.max_concurrent_handoffs).unwrap_or(usize::MAX),
         )),
     };
-    // This node alone until the gossip runs.
-    let me = Member {
-        addr: ([0, 0, 0, 0], 0).into(),
-        meta: NodeMeta {
-            phase: Phase::Active,
-            config_generation: 0,
-            build: env!("CARGO_PKG_VERSION").to_owned(),
-        },
-        suspect: false,
-    };
-    let (_alone, members) = watch::channel(Members {
-        me: deps.holder.node_id.clone(),
-        nodes: [(deps.holder.node_id.clone(), me)].into(),
-    });
     let mut pipelines = Vec::new();
     for p in config.pipelines.values() {
         pipelines.push((p.clone(), buckets.pipeline_docs(p)?));
     }
-    let reconciler =
-        ClusterReconciler::start(pipelines, &deps, members, ReconcilerSettings::new(c));
+    // The gossip (Clustering §2–§3): joining until a peer answers, then active.
+    let m = &c.memberlist;
+    let bind: Vec<SocketAddr> = m
+        .bind_addr
+        .iter()
+        .map(|ip| SocketAddr::new(*ip, m.bind_port))
+        .collect();
+    let (udp, inbox) = Udp::bind(&bind)
+        .await
+        .map_err(|e| Fatal::Startup(e.to_string()))?;
+    let addr = advertise(m, &interfaces())
+        .await
+        .map_err(|e| Fatal::Config(e.to_string()))?;
+    let mut own = udp.local_addrs();
+    own.push(addr);
+    let me = NodeIdentity {
+        node_id: deps.holder.node_id.clone(),
+        incarnation: deps.holder.incarnation,
+        rev: 0,
+        addr,
+        meta: NodeMeta {
+            phase: Phase::Joining,
+            config_generation: joined.generation,
+            build: env!("CARGO_PKG_VERSION").to_owned(),
+        },
+    };
+    let key = GossipKey::derive(&joined.fingerprint_key);
+    let gossip = Gossip::start(me, key, GossipSettings::new(m), Arc::new(udp), inbox);
+    tracing::info!(%addr, bound = ?bind, "gossip started");
+    let seeds = Seeds::new(c);
+    let seeded = !seeds.is_empty();
+    let discovery = discover(seeds, own, gossip.announcer(), gossip.members());
+    join_gossip(&gossip, seeded).await;
+    let reconciler = ClusterReconciler::start(
+        pipelines,
+        &deps,
+        gossip.members(),
+        ReconcilerSettings::new(c),
+    );
     ready.store(true, Ordering::SeqCst);
     shutdown_signal().await;
-    tracing::info!("shutting down: draining every pipeline and releasing its lease");
+    tracing::info!("shutting down: handing off every pipeline and releasing its lease");
     ready.store(false, Ordering::SeqCst);
-    reconciler.shutdown().await;
+    discovery.abort();
+    leave(gossip, reconciler).await;
     let _ = stop_http.send(());
     let _ = server.await;
     tracing::info!("stopped");
```

Change `crates/data-avalanche/src/startup.rs`:

```diff
diff --git a/crates/data-avalanche/src/startup.rs b/crates/data-avalanche/src/startup.rs
index c9095c8..776fe0c 100644
--- a/crates/data-avalanche/src/startup.rs
+++ b/crates/data-avalanche/src/startup.rs
@@ -105,6 +105,10 @@ impl Buckets {
 pub(crate) struct Joined {
     pub config: ResolvedConfig,
     pub globals: DocStore,
+    /// The anchor's: the gossip's packets are authenticated with a key derived from it.
+    pub fingerprint_key: [u8; 32],
+    /// The config generation the node runs.
+    pub generation: u64,
 }
 
 /// Steps 2–5 of the startup sequence; `first` is the config loaded with a throwaway key.
@@ -167,7 +171,12 @@ pub(crate) async fn join(
             named(&adopted)
         )));
     }
-    Ok(Joined { config, globals })
+    Ok(Joined {
+        config,
+        globals,
+        fingerprint_key: anchor.fingerprint_key,
+        generation: generation.pointer.doc.generation,
+    })
 }
 
 #[cfg(test)]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche --lib`

Expected: PASS, 15 tests.

```text
test result: ok. 15 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
```

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 lifecycle::`

Expected: PASS, 3 tests.

```text
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 30 filtered out; finished in 56.09s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1139 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/discovery.rs \
  crates/data-avalanche-cluster/src/wire.rs \
  crates/data-avalanche/src/run.rs \
  crates/data-avalanche/src/startup.rs \
  crates/data-avalanche/tests/e2e/harness.rs
git commit -m "feat(data-avalanche): the node in the cluster — the gossip on its UDP port with the anchor's key, discovery and the join, the ring's reconciler, the leave on SIGTERM; E2E nodes take their name and ports from the environment (D-3)"
```

---

### Task 12: Three nodes as processes (`tests/e2e`; Q-M8-4)

**Files:**
- Create (test): `crates/data-avalanche/tests/e2e/cluster.rs`
- Create (test): `crates/data-avalanche/tests/e2e/forwarder.rs`
- Modify (test): `crates/data-avalanche/tests/e2e/harness.rs`
- Modify (test): `crates/data-avalanche/tests/e2e/main.rs`
- Modify: `test-infra/README.md`

**Interfaces:**
- Consumes: the harness of Task 11 (`spawn`, `free_udp_port`, `Proc`); M6b/M7b's `E2e`, `Db`, `mirrored`, metrics helpers.
- Produces:
  - `forwarder.rs`: `async Forwarder::start(to: u16) -> Forwarder`, `port(&self) -> u16`, `block(&self, from: &[u16])`, `heal(&self)`
  - `harness.rs`: `Options.cluster`, `Options.memberlist` (flow entries); `start_node_with(node, env)`; `Proc::signal(&self, signal: &str)`; `async lease_of(&self, id: &str) -> Option<Versioned<LeaseDoc>>`; `async drop_source(&self, suffix: &str)`
  - `cluster.rs`: a `Cluster` of three nodes (pipelines `test`, `b`, `c`), `settled()`, `cut(node)`, `heal()`, `landed()`, and the four scenarios

D-M8a.20, Q-M8-4. Each node gossips on its own loopback port and advertises a forwarder's port. Every packet to the node passes the forwarder, which can drop by source port: a partition between real processes, with the storage reachable. `kill -9` is a crash; SIGTERM is the leave; SIGSTOP and SIGCONT are a stop-the-world pause past the lease. A scenario has settled when every live node's leases match the ring over the live nodes, and the nodes' `cluster_members` and owned metrics agree. No double commit: each table's commits carry non-decreasing `data-avalanche.owner_epoch`. The scenarios need only the source and RustFS, and run on either source. The docker-compose 3-node run of the roadmap's exit moves to M11's kind smoke (Q-M8-4).

- [ ] **Step 1: Write the tests**

Create `crates/data-avalanche/tests/e2e/cluster.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/cluster.rs b/crates/data-avalanche/tests/e2e/cluster.rs
new file mode 100644
index 0000000..076ca6b
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/cluster.rs
@@ -0,0 +1,348 @@
+//! Three nodes as three processes on the host (Q-M8-4): each gossips on its own loopback port
+//! behind a [`Forwarder`], seeded with every node's forwarder. Three pipelines over one table
+//! spread over them. A crash (`kill -9`), a leave (SIGTERM), a partition (the forwarders drop a
+//! side's packets) and a stop-the-world pause (SIGSTOP) each end with every pipeline held by one
+//! live node, every row in the lake, and owner epochs that never go back in the Delta log.
+
+use std::collections::BTreeMap;
+use std::time::{Duration, Instant};
+
+use crate::forwarder::Forwarder;
+use data_avalanche_cluster::Ring;
+use data_avalanche_testkit::require_it;
+
+use crate::harness::{E2e, Options, Proc, eventually, free_udp_port};
+
+const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";
+
+/// The test's pipeline and two more over the same table, each into a lake of its own.
+const MORE: &str = r#"  b:
+    id: b
+@SOURCE(_b)@
+    tables: { include: ['@INCLUDE@'], snapshots: { mode: never } }
+    sink: { type: delta, bucket: @BUCKET@, prefix: @PREFIX@/lake_b, delta: {}, flush: { seconds: 1 } }
+  c:
+    id: c
+@SOURCE(_c)@
+    tables: { include: ['@INCLUDE@'], snapshots: { mode: never } }
+    sink: { type: delta, bucket: @BUCKET@, prefix: @PREFIX@/lake_c, delta: {}, flush: { seconds: 1 } }
+"#;
+
+struct Node {
+    name: String,
+    /// The port the node gossips from (its bind port).
+    gossip: u16,
+    fwd: Forwarder,
+    proc: Option<Proc>,
+}
+
+/// The nodes are dropped (and print their logs on a failure) before the test's directory.
+struct Cluster {
+    nodes: Vec<Node>,
+    e: E2e,
+}
+
+impl Cluster {
+    /// `n1`, `n2` and `n3`, not started.
+    async fn new(test: &str) -> Self {
+        let mut nodes = Vec::new();
+        for name in ["n1", "n2", "n3"] {
+            let gossip = free_udp_port();
+            let fwd = Forwarder::start(gossip).await;
+            nodes.push(Node {
+                name: name.to_owned(),
+                gossip,
+                fwd,
+                proc: None,
+            });
+        }
+        let seeds: Vec<String> = nodes
+            .iter()
+            .map(|n| format!("\"127.0.0.1:{}\"", n.fwd.port()))
+            .collect();
+        let memberlist = format!(
+            ", push_pull_interval: 5s, join_members: [{}]",
+            seeds.join(", ")
+        );
+        let e = E2e::new(
+            test,
+            ITEMS,
+            Options {
+                tables: ", snapshots: { mode: never }",
+                cluster: ", rebalance_stabilization: 3s",
+                memberlist: &memberlist,
+                pipelines: MORE,
+                ..Options::default()
+            },
+        )
+        .await;
+        for suffix in ["_b", "_c"] {
+            e.drop_source(suffix).await;
+        }
+        Self { nodes, e }
+    }
+
+    async fn start(&mut self, i: usize) {
+        let n = &mut self.nodes[i];
+        let (gossip, advertise) = (n.gossip.to_string(), n.fwd.port().to_string());
+        let mut p = self.e.start_node_with(
+            &n.name,
+            &[
+                ("E2E_GOSSIP_PORT", &gossip),
+                ("E2E_ADVERTISE_PORT", &advertise),
+            ],
+        );
+        p.ready().await;
+        n.proc = Some(p);
+    }
+
+    async fn start_all(&mut self) {
+        for i in 0..self.nodes.len() {
+            self.start(i).await;
+        }
+        self.settled().await;
+    }
+
+    fn ids(&self) -> [String; 3] {
+        [self.e.test.clone(), "b".into(), "c".into()]
+    }
+
+    /// Each pipeline's lease: its holder and epoch, unless released.
+    async fn holders(&self) -> BTreeMap<String, (String, u64)> {
+        let mut by = BTreeMap::new();
+        for id in self.ids() {
+            if let Some(l) = self.e.lease_of(&id).await
+                && !l.doc.released
+            {
+                by.insert(id, (l.doc.holder_node_id, l.doc.owner_epoch));
+            }
+        }
+        by
+    }
+
+    fn live(&self) -> Vec<&Node> {
+        self.nodes.iter().filter(|n| n.proc.is_some()).collect()
+    }
+
+    fn index(&self, name: &str) -> usize {
+        self.nodes
+            .iter()
+            .position(|n| n.name == name)
+            .expect("a node")
+    }
+
+    /// Waits until every live node sees the others and holds what its metric says, and each
+    /// pipeline is held by the node the ring over the live nodes places it on.
+    async fn settled(&self) {
+        let live = self.live();
+        let ring = Ring::new(live.iter().map(|n| n.name.as_str()), 128);
+        eventually("the cluster settles", Duration::from_secs(90), async || {
+            let holders = self.holders().await;
+            let placed = self
+                .ids()
+                .iter()
+                .all(|id| holders.get(id).map(|(h, _)| h.as_str()) == ring.owner(id));
+            if !placed {
+                return false;
+            }
+            for n in &live {
+                let text = n.proc.as_ref().unwrap().metrics().await;
+                let held = holders.values().filter(|(h, _)| *h == n.name).count();
+                let lines = [
+                    format!("data_avalanche_cluster_members {}", live.len()),
+                    format!("data_avalanche_owned_pipelines {held}"),
+                ];
+                if !lines.iter().all(|l| text.lines().any(|t| t == l)) {
+                    return false;
+                }
+            }
+            true
+        })
+        .await;
+    }
+
+    /// Cuts `side` off from every other node, both ways.
+    fn cut(&self, side: &[&str]) {
+        let (a, b): (Vec<&Node>, Vec<&Node>) = self
+            .nodes
+            .iter()
+            .partition(|n| side.contains(&n.name.as_str()));
+        for x in &a {
+            x.fwd.block(&b.iter().map(|n| n.gossip).collect::<Vec<_>>());
+        }
+        for y in &b {
+            y.fwd.block(&a.iter().map(|n| n.gossip).collect::<Vec<_>>());
+        }
+    }
+
+    fn heal(&self) {
+        for n in &self.nodes {
+            n.fwd.heal();
+        }
+    }
+
+    /// The lake's `items` holds the source's rows, and its owner epochs never went back.
+    async fn landed(&self) {
+        self.e.converged("items").await;
+        let epochs: Vec<u64> = self
+            .e
+            .log("items")
+            .await
+            .iter()
+            .flatten()
+            .filter_map(|a| a["commitInfo"]["data-avalanche.owner_epoch"].as_u64())
+            .collect();
+        assert!(!epochs.is_empty(), "the commits carry their epoch");
+        assert!(epochs.is_sorted(), "an epoch went back: {epochs:?}");
+    }
+
+    async fn stop(mut self) {
+        for n in &mut self.nodes {
+            if let Some(p) = n.proc.take() {
+                assert!(p.terminate().await.success(), "{} stops", n.name);
+            }
+        }
+        for suffix in ["_b", "_c"] {
+            self.e.drop_source(suffix).await;
+        }
+        self.e.cleanup().await;
+    }
+}
+
+/// The holder of the test's own pipeline is killed: its pipelines go to the others once their
+/// leases ran out (suspicion 4 s, lease 6 s), at the next epoch, and every row lands.
+#[tokio::test]
+async fn a_killed_nodes_pipelines_are_taken_over() {
+    require_it!();
+    let mut c = Cluster::new("cluster_kill").await;
+    c.start_all().await;
+    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
+    c.landed().await;
+    let before = c.holders().await;
+    let (victim, epoch) = before[&c.e.test].clone();
+    let i = c.index(&victim);
+    let t = Instant::now();
+    assert!(!c.nodes[i].proc.take().unwrap().kill().await.success());
+    c.e.exec("INSERT INTO items VALUES (2, 'b')").await;
+    c.settled().await;
+    let took = t.elapsed();
+    let after = c.holders().await;
+    for (id, (holder, e)) in &before {
+        if *holder == victim {
+            assert_ne!(after[id].0, victim);
+            assert_eq!(after[id].1, e + 1, "{id}: the next epoch");
+        } else {
+            assert_eq!(after[id], before[id], "{id} stays");
+        }
+    }
+    assert_eq!(after[&c.e.test].1, epoch + 1);
+    assert!(
+        took >= Duration::from_secs(6),
+        "{took:?}: before the lease ran out"
+    );
+    c.landed().await;
+    c.stop().await;
+}
+
+/// Each node in turn leaves (SIGTERM) and starts again: a leave hands its pipelines off
+/// without a lease wait, and the rows inserted throughout all land.
+#[tokio::test]
+async fn a_rolling_restart_hands_off_and_loses_nothing() {
+    require_it!();
+    let mut c = Cluster::new("cluster_roll").await;
+    c.start_all().await;
+    for (i, row) in (0..3).zip(1..) {
+        let name = c.nodes[i].name.clone();
+        let t = Instant::now();
+        assert!(
+            c.nodes[i].proc.take().unwrap().terminate().await.success(),
+            "{name}"
+        );
+        c.settled().await;
+        let took = t.elapsed();
+        assert!(
+            took < Duration::from_secs(6),
+            "{name}'s hand-off took {took:?}: a lease wait"
+        );
+        c.e.exec(&format!("INSERT INTO items VALUES ({row}, 'r')"))
+            .await;
+        c.start(i).await;
+        c.settled().await;
+    }
+    c.landed().await;
+    c.stop().await;
+}
+
+/// `{n1, n2} | {n3}`: each side sees the other gone, but every lease is still renewed, so no
+/// pipeline moves; after the heal the members are whole and the leases where they were.
+#[tokio::test]
+async fn a_partition_moves_no_pipeline() {
+    require_it!();
+    let mut c = Cluster::new("cluster_split").await;
+    c.start_all().await;
+    let before = c.holders().await;
+    c.cut(&["n3"]);
+    let n3 = c.nodes[2].proc.as_ref().unwrap();
+    n3.metric("data_avalanche_cluster_members 1").await;
+    c.nodes[0]
+        .proc
+        .as_ref()
+        .unwrap()
+        .metric("data_avalanche_cluster_members 2")
+        .await;
+    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
+    let t = Instant::now();
+    while t.elapsed() < Duration::from_secs(15) {
+        assert_eq!(
+            c.holders().await,
+            before,
+            "a lease moved during the partition"
+        );
+        tokio::time::sleep(Duration::from_millis(500)).await;
+    }
+    c.e.converged("items").await;
+    c.heal();
+    c.settled().await;
+    assert_eq!(c.holders().await, before, "the heal moved a lease");
+    c.landed().await;
+    c.stop().await;
+}
+
+/// The holder of the test's pipeline stops for 15 s (SIGSTOP), past its lease: another node
+/// takes it at the next epoch while it is frozen. Back (SIGCONT), the paused node finds its
+/// lease lost and its commits fenced; nothing it wrote goes behind the new owner's epoch, and
+/// it rejoins.
+#[tokio::test]
+async fn a_paused_holder_loses_its_pipelines_and_rejoins() {
+    require_it!();
+    let mut c = Cluster::new("cluster_pause").await;
+    c.start_all().await;
+    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
+    c.landed().await;
+    let (victim, epoch) = c.holders().await[&c.e.test].clone();
+    let i = c.index(&victim);
+    c.nodes[i].proc.as_ref().unwrap().signal("STOP");
+    c.e.exec("INSERT INTO items VALUES (2, 'b')").await;
+    eventually(
+        "taken over while paused",
+        Duration::from_secs(30),
+        async || {
+            c.holders()
+                .await
+                .get(&c.e.test)
+                .is_some_and(|(_, e)| *e > epoch)
+        },
+    )
+    .await;
+    // The paused node's replication connection stays open, so on PostgreSQL its walsender keeps
+    // the slot until `wal_sender_timeout` (60 s) ends it; the new holder streams at its next
+    // busy-slot retry after that, at most a backoff step (60 s) later (`Postgres_Slot…` D-15.5).
+    c.e.converged_within("items", Duration::from_secs(150))
+        .await;
+    tokio::time::sleep(Duration::from_secs(5)).await;
+    c.nodes[i].proc.as_ref().unwrap().signal("CONT");
+    c.e.exec("INSERT INTO items VALUES (3, 'c')").await;
+    c.settled().await;
+    c.landed().await;
+    c.stop().await;
+}
```

Create `crates/data-avalanche/tests/e2e/forwarder.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/forwarder.rs b/crates/data-avalanche/tests/e2e/forwarder.rs
new file mode 100644
index 0000000..59ef45f
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/forwarder.rs
@@ -0,0 +1,69 @@
+//! A UDP forwarder in front of a node's gossip port (Q-M8-4). The node advertises the
+//! forwarder's port, so every gossip packet to it passes here; the gossip replies to the
+//! address a node advertises, never to a packet's source, so forwarding one way is enough. A
+//! packet's source port is the sending node's own gossip port: dropping chosen sources cuts
+//! those nodes off from this one, a partition no process notices but by its silence.
+
+use std::collections::HashSet;
+use std::sync::{Arc, Mutex};
+
+use tokio::net::UdpSocket;
+use tokio::task::AbortHandle;
+
+/// The forwarder, forwarding until dropped.
+#[derive(Debug)]
+pub struct Forwarder {
+    port: u16,
+    blocked: Arc<Mutex<HashSet<u16>>>,
+    task: AbortHandle,
+}
+
+impl Forwarder {
+    /// Forwards what `127.0.0.1:<port>` receives to `127.0.0.1:<to>`.
+    pub async fn start(to: u16) -> Self {
+        let socket = UdpSocket::bind("127.0.0.1:0")
+            .await
+            .expect("a forwarder port");
+        let port = socket.local_addr().expect("its address").port();
+        let blocked: Arc<Mutex<HashSet<u16>>> = Arc::default();
+        let b = Arc::clone(&blocked);
+        let task = tokio::spawn(async move {
+            let mut buf = vec![0u8; 2048];
+            loop {
+                let Ok((n, from)) = socket.recv_from(&mut buf).await else {
+                    continue;
+                };
+                if b.lock().unwrap().contains(&from.port()) {
+                    continue;
+                }
+                let _ = socket.send_to(&buf[..n], ("127.0.0.1", to)).await;
+            }
+        })
+        .abort_handle();
+        Self {
+            port,
+            blocked,
+            task,
+        }
+    }
+
+    /// The port the node advertises.
+    pub fn port(&self) -> u16 {
+        self.port
+    }
+
+    /// Drops the packets the nodes gossiping from `ports` send to this one.
+    pub fn block(&self, ports: &[u16]) {
+        self.blocked.lock().unwrap().extend(ports);
+    }
+
+    pub fn heal(&self) {
+        self.blocked.lock().unwrap().clear();
+    }
+}
+
+impl Drop for Forwarder {
+    fn drop(&mut self) {
+        self.task.abort();
+    }
+}
```

Change `crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
index 2329724..c9846a6 100644
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -73,6 +73,11 @@ pub struct Options<'a> {
     /// Pipeline keys beyond `source`, `tables` and `sink`, one YAML line, e.g.
     /// `schema_evolution: { rescan_interval: 2s }`.
     pub pipeline: &'a str,
+    /// `globals.cluster` entries, each `, key: value` a line of the block, e.g.
+    /// `, rebalance_stabilization: 3s`.
+    pub cluster: &'a str,
+    /// `globals.cluster.memberlist` entries, e.g. `, join_members: ["127.0.0.1:7946"]`.
+    pub memberlist: &'a str,
     /// More pipelines, YAML under `pipelines:` (2-space indent); `@SCHEMA@`, `@PREFIX@` and
     /// `@BUCKET@` are replaced, `@INCLUDE@` by the pattern of every table of the test, and a line
     /// `@SOURCE(<suffix>)@` by the source block ([`E2e::source_block`]).
@@ -248,8 +253,8 @@ globals:
   cluster:
     lease_duration: 6s
     lease_renewal_interval: 2s
-    drain_timeout: 30s
-    memberlist: {{ node_name: "${{E2E_NODE}}", bind_addr: [127.0.0.1], bind_port: "${{E2E_GOSSIP_PORT}}", advertise_addr: 127.0.0.1, advertise_port: "${{E2E_ADVERTISE_PORT}}" }}
+    drain_timeout: 30s{cluster}
+    memberlist: {{ node_name: "${{E2E_NODE}}", bind_addr: [127.0.0.1], bind_port: "${{E2E_GOSSIP_PORT}}", advertise_addr: 127.0.0.1, advertise_port: "${{E2E_ADVERTISE_PORT}}"{memberlist} }}
 pipelines:
   {test}:
     id: {test}
@@ -266,6 +271,8 @@ pipelines:
             tables = self.qualify(o.tables),
             sink = o.sink,
             pipeline = o.pipeline,
+            cluster = o.cluster.replace(", ", "\n    "),
+            memberlist = o.memberlist,
         )
     }
 
@@ -276,7 +283,12 @@ pipelines:
 
     /// Starts the binary as another node, with its own name and API port.
     pub fn start_node(&self, node: &str) -> Proc {
-        self.spawn(node, free_port(), &[])
+        self.start_node_with(node, &[])
+    }
+
+    /// [`Self::start_node`] with more variables (a node's gossip ports).
+    pub fn start_node_with(&self, node: &str, env: &[(&str, &str)]) -> Proc {
+        self.spawn(node, free_port(), env)
     }
 
     /// Runs node `node` with its API on `port`, gossiping alone on a loopback port of its own;
@@ -439,12 +451,14 @@ pipelines:
 
     /// Waits until the lake's `table` has exactly the source's ids.
     pub async fn converged(&self, table: &str) {
+        self.converged_within(table, Duration::from_secs(60)).await;
+    }
+
+    /// [`Self::converged`], waiting at most `within`.
+    pub async fn converged_within(&self, table: &str, within: Duration) {
         let want = self.source_ids(table).await;
         let what = format!("{table} has the source's {} rows", want.len());
-        eventually(&what, Duration::from_secs(60), async || {
-            self.ids(table).await == want
-        })
-        .await;
+        eventually(&what, within, async || self.ids(table).await == want).await;
     }
 
     /// Whether the source streams to the pipeline now: PostgreSQL's slot has a walsender,
@@ -575,12 +589,31 @@ pipelines:
 
     /// The pipeline's lease in `globals.state`.
     pub async fn lease(&self) -> Option<Versioned<LeaseDoc>> {
+        self.lease_of(&self.test).await
+    }
+
+    /// Pipeline `id`'s lease in `globals.state`.
+    pub async fn lease_of(&self, id: &str) -> Option<Versioned<LeaseDoc>> {
         Leases::new(self.docs_at(&format!("{}/globals", self.prefix)))
-            .observe(&self.test)
+            .observe(id)
             .await
             .expect("the lease reads")
     }
 
+    /// Drops the slot and publication of the pipeline whose source block took `suffix`
+    /// ([`Options::pipelines`]' `@SOURCE(<suffix>)@`), on PostgreSQL; [`Self::cleanup`] drops
+    /// only the test's own.
+    pub async fn drop_source(&self, suffix: &str) {
+        if let Source::Postgres { .. } = self.source {
+            let name = format!("{}{suffix}", self.schema);
+            drop_slot(&self.db, &name).await;
+            self.db
+                .exec(&format!("DROP PUBLICATION IF EXISTS {name}_pub"))
+                .await
+                .expect("the publication drops");
+        }
+    }
+
     /// The pipeline's own documents, co-located with the lake (`<sink.prefix>/.pipeline-state`).
     pub fn docs(&self) -> PipelineDocs {
         let at = colocated_state(TEST_BUCKET, &self.lake_prefix());
@@ -836,6 +869,16 @@ impl Proc {
         self.exit().await
     }
 
+    /// Sends `signal` (`STOP`, `CONT`): a stop-the-world pause and its end.
+    pub fn signal(&self, signal: &str) {
+        let pid = self.child.id().to_string();
+        let sent = Command::new("kill")
+            .args([&format!("-{signal}"), &pid])
+            .status()
+            .expect("kill runs");
+        assert!(sent.success(), "kill -{signal}");
+    }
+
     /// `kill -9`, as a host failure would.
     pub async fn kill(mut self) -> ExitStatus {
         let pid = self.child.id().to_string();
```

Change `crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index 1035625..6edb979 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -6,7 +6,9 @@
 //!
 //! `cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`
 
+mod cluster;
 mod crashes;
+mod forwarder;
 mod guards;
 mod harness;
 mod lifecycle;
```

- [ ] **Step 2: Build the suite**

Run: `cargo build -q -p data-avalanche --features e2e-hooks --tests`

Expected: it builds. The behaviour is Task 11's binary, so the scenarios pass on arrival (Step 4).

```text

```

- [ ] **Step 3: Document the suite**

Change `test-infra/README.md`:

```diff
diff --git a/test-infra/README.md b/test-infra/README.md
index 6ed2096..ed21cfd 100644
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -91,6 +91,13 @@ the Docker CLI. The faults no server can be asked for (a corrupted compressed tr
 failover behind one address) come from a test-only TCP proxy in the suite (`proxy.rs`), which
 reads the protocol in the clear: the suite's pipelines use `ssl_mode: disable`.
 
+Each binary the suite starts is a node of its own: its name, API port and gossip port come from
+`E2E_*` variables, as a cluster's one config takes them from each pod's environment (a node runs
+the cluster's config generation, not its own file). The cluster scenarios (`cluster.rs`) run
+three nodes as three processes on loopback, each behind a test-only UDP forwarder
+(`forwarder.rs`) that drops a chosen side's gossip (a partition); they also `kill -9`, SIGTERM
+and SIGSTOP the nodes. They need no service beyond the source and RustFS.
+
 ## GCS: a real bucket, not an emulator
 
 `fake-gcs-server` 1.56.1 rejects `object_store`'s path-style PUT (`400 invalid uploadType`) and
```

- [ ] **Step 4: Run the scenarios**

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 cluster::`

Expected: PASS, 4 tests.

```text
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 33 filtered out; finished in 190.31s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1139 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche/tests/e2e/cluster.rs \
  crates/data-avalanche/tests/e2e/forwarder.rs \
  crates/data-avalanche/tests/e2e/harness.rs \
  crates/data-avalanche/tests/e2e/main.rs \
  test-infra/README.md
git commit -m "test(e2e): three nodes as processes — a kill taken over after the lease, a rolling restart handed off, a partition behind UDP forwarders that moves nothing, a SIGSTOP past the lease (Q-M8-4)"
```

---

## Exit

- [ ] Three nodes spread their pipelines by the ring, and each pipeline runs on one node (Tasks 10, 12).
- [ ] A killed node's pipelines are taken over after the lease, with no double commit (Task 12).
- [ ] A leave hands every pipeline off without a lease wait; a rolling restart loses nothing, with hand-offs capped (Tasks 6, 7, 12).
- [ ] A partition with storage reachable moves no pipeline; a pause past the lease is fenced and rejoins (Tasks 10, 12).
- [ ] A node of another cluster can't join (Tasks 3, 4).
- [ ] DA-CLUS-14 (Task 8) and DA-OPS-7 (Task 6) are closed.
- [ ] The E2E suite passes on PostgreSQL 18 and, through `ci.yml`'s jobs, on `mysql-84`, `mysql-84-nogtid` and `mariadb-118`.
- [ ] Every Review Focus line has its passing test.
- [ ] The documents of "Documents this plan changes" are updated, and the spec patch is applied once approved.
- [ ] The workspace gates pass: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`, `cargo test --workspace`.

## Deviations during execution

None yet.
