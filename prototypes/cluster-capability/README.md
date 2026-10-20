# cluster-capability — the gossip membership layer

Companion to `lease-conformance/`, `iceberg-capability/`, `delta-capability/` and
`source-capability/`. It is the **M8 probe gate** (roadmap §2.1, risk R-1): can the cluster's
coordination plane (`Clustering_Architecture_Research.md` §2–§3) be built on a pinned crate?
The candidates are `memberlist` 0.8.5 (the roadmap's pick) and `foca` 2.0 (its fallback).

Status: **run 2026-10-20 on loopback (macOS, tokio multi-thread).** `memberlist` 0.8.5 **fails
the gate**: a bookkeeping bug corrupts its member table after dead nodes are reaped (C13).
`foca` 2.0, driven by a ~250-line tokio actor of ours, **passes every gate** (F1…F8). Oleksii's
decision (Q-M8-1, 2026-10-20): **build on foca 2.0.**

## Verdict

| Need (Clustering §2–§3, §8, §20.3) | memberlist 0.8.5 | foca 2.0 + our actor |
|---|---|---|
| Join through a seed, converge (3 nodes) | ✅ 0.31 s (C1) | ✅ 0.21 s over UDP (F1) |
| Node metadata, changed at run time | ✅ 512 B max, a bigger value **panics**; 0.25–0.39 s (C2) | ✅ carried in the identity (85 B encoded, F8); 0.02 s (F2) |
| Graceful leave | ✅ out of the online set in 0.21 s (C3) | ✅ 0.02 s (F3) |
| Kill → suspect → dead | ✅ offline after 6.1 s (C4); suspect **not observable** | ✅ suspect 1.8 s, down 5.8 s (F4), state public |
| Restart under the same name, same address | ✅ 0.26 s (C5) | ✅ (F5 covers the harder case) |
| Restart at a **new address** (a pod's new IP) | ⚠️ 37–51 s under `dead_node_reclaim_time` 30 s / 0; immediate with 1 s and a 2 s restart delay (C5, C5b) | ✅ 0.02–0.23 s: the higher incarnation wins (F5) |
| Partition {a,b} \| {c}, then heal | not probed (gate already failed) | ✅ split seen in 6.8 s, healed in 1.0 s (F6, in-memory hub) |
| Stop-the-world pause | not probed | ✅ 1.5 s: no effect; 15 s: down after 5.8–6.8 s, back 8–9 s after it resumes (F7) |
| Member table stays consistent | ❌ **`by_id` panics** "index out of bounds: the len is 4 but the index is 5" in round 3 of 6 (C13); the same lookup panicked inside memberlist's alive-message handler once (C5b) | ✅ (no index map; state is foca's own) |
| Lifeguard (local health awareness) | ✅ always on (`awareness_max_multiplier`; no off switch) | ❌ none: SWIM + infection + suspicion only |
| TLS for gossip | ⚠️ the `Tls` stream layer covers TCP push/pull only: `packet_secure() = false`, UDP probes and gossip stay plaintext and unauthenticated unless the `encryption` keyring (a shared key) is on | n/a: we authenticate our own packets |
| Maintained line | ⚠️ 0.8.5 is the last release of the tokio line; upstream `main` is an unreleased rewrite (sans-IO reactor, compio, embedded drivers) with no `memberlist-net` | ✅ 2.0.0 (2026-08-02), 256k downloads, sans-IO by design |

## Findings

1. **memberlist's reaping corrupts its id → index map (C13).** `memberlist-core` 0.8.5 keeps
   `node_map: id → index into nodes`. `move_dead_nodes` (`state.rs`, called by `reset_nodes` when
   the probe index wraps) swaps entries of `nodes` without updating `node_map`; `reset_nodes` then
   pops the tail and the shuffle that follows rewrites only the entries it touches. An entry can
   stay stale: `by_id` (public) and the alive handler (`state.rs:444`,
   `&memberlist.nodes[*idx]`) then index past the end and panic, or read another node's state. The
   Go original maps a name to a pointer, so it doesn't have this bug; the Rust port introduced it.
   A panic in the alive handler ends that task silently. Not reported upstream (an outward-facing
   action; noted for Oleksii).
2. **memberlist hides member states.** `members()` returns the `NodeState` built from the last
   alive message, so `state()` reads `Alive` for a suspect, dead or left node (C3), and
   `notify_leave` hands over the same snapshot: a delegate can't tell a leave from a death. Only
   `online_members()` (alive or suspect) filters on the private state.
3. **memberlist's `dead_node_reclaim_time` delays a pod that restarts with a new IP (C5b).** A
   name at a new address is refused as a conflict until the dead entry is reclaimable (or reaped
   after `gossip_to_the_dead_time`): 51 s at the spec default 30 s, 39 s at 0 ("never").
4. **foca keys members by address, not by name.** An `Announce` is accepted only when the
   destination's `addr()` equals the receiver's own (`accept_payload`), and a seed is known only by
   its address. So `Identity::Addr` is the socket address; our layer groups identities by node
   name and takes the highest `(incarnation, rev)` as the node's current one. A restarted node's
   old identity lingers as `Suspect` until it is declared down (F5's view), which the grouping
   hides.
5. **foca's identity carries the metadata.** A change is `change_identity` with the next `rev`; peers
   see `Rename(old, new)` and no departure (F2). `renew()` bumps `rev`, so a member declared down
   while alive (a partition, a pause) rejoins at once with a new identity (F6, F7: `Rejoin`).
6. **foca's defaults announce to down members every 65 s.** The probe uses 5 s; with 65 s a
   healed partition waits up to a minute. The cadence is ours to set (`periodic_announce`,
   `periodic_announce_to_down_members`).
7. **A partition and a pause are cheap to test with foca.** The probe's in-memory hub drops
   chosen (from, to) pairs, and a paused actor stops processing while its inbox fills. foca replies
   to the address in the sender's identity, not to a UDP source address, so a plain per-node UDP
   forwarder can cut a real process's traffic in an end-to-end test.

## Gates

| ID | Probe | File |
|---|---|---|
| C1 | 3 nodes join through one seed; metadata visible | `tests/memberlist_c1_join.rs` |
| C2–C5 | metadata update; leave; kill; restart at the same and a new address | `tests/memberlist_c2_lifecycle.rs` |
| C5b | new-address rejoin under `dead_node_reclaim_time` 1 s ×3, 0, 30 s | `tests/memberlist_c5b_reclaim.rs` |
| C13 | `by_id` against `members` after reaping (6 rounds, 5 joiners, 3 killed) | `tests/memberlist_c13_reap.rs` |
| F1…F8 | join over UDP; metadata; leave; kill; restart at a new address; partition and heal; pauses; identity size | `tests/foca_gates.rs` |

The memberlist node (`src/memberlist_node.rs`): `NetTransport` over TCP/UDP on loopback, a
`NodeDelegate` serving the metadata and a `SubscribleEventDelegate`; SWIM timers near config
§6.5.1's defaults (probe 1 s, timeout 500 ms, gossip 200 ms). The foca node
(`src/foca_node.rs`): one actor per node owning `Foca`, fed with received packets, timer events
(a `tokio::time::sleep` per scheduled timer) and commands; its wire is a UDP socket or the
in-memory hub; `Config::new_lan(3)` with announces to down members every 5 s.
