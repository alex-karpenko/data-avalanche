---
title: Clustering Architecture Research
status: Research / Design proposal (input for further elaboration)
updated: 2026-06-20
domain: Architecture / Distributed Systems
tags: [clustering, gossip, swim, memberlist, consistent-hashing, lease, split-brain, rebalancing, config-propagation]
closes_or_advances: [G-8, G-9, G-2, G-17, G-23, G-36, "OQ-CL1", "OQ-CL3", "OQ-CL4", "OQ-API3", "OQ-P3", "5.1", "5.2"]
sources: [SWIM (Cornell), memberlist crate, Grafana Loki/Mimir hash rings, Quickwit Scuttlebutt, Akka Cluster Sharding, Consistent Hashing, hashring crate, S3 conditional writes]
---

# Clustering Architecture Research

## 0. Purpose and scope

This document researches the clustering design for the CDC system and proposes a concrete
architecture for the six clustering responsibilities raised in the brief:

1. Maintain the live membership list with activity / reachability / phase status, and a reliable
   coordinated join / leave / status-update mechanism.
2. Maintain a consistent-hash ring of virtual-node tokens that spreads pipelines evenly and
   transfers them smoothly off leaving or dead nodes.
3. Detect dead / unreachable nodes automatically, move their pipelines to live nodes, and
   rebalance the workload.
4. Durably guarantee that **exactly one** node runs each pipeline, **and** that **every** pipeline
   is running.
5. Store and maintain global and per-pipeline configuration "in the ring," including propagation
   of change notifications.
6. Treat a single-node deployment as a one-member cluster that can become a real multi-node
   cluster at any moment.

It plugs directly into the contracts already fixed in `Configuration_Specification.md` (the
`globals.cluster`, `globals.cluster.memberlist`, and `globals.state` sections, the `tokens` anchor,
the R/PR/PROC mutability model) and `Data-Flow.md` (offset checkpointing, the worker-failure path,
the shutdown sequence). It is intended to **close G-8 (split-brain lease) and G-9 (rebalancing
drain)**, **complete G-2 / G-17 (cross-worker config propagation and reload proxy)**, **record the
G-23 (SWIM vs Scuttlebutt) decision**, and **detail the `globals.state` layout (gap 5.1) and atomic
state-update mechanism (gap 5.2)**. G-36 (cluster-wide token-count consistency) is already resolved
in the config spec; §3.3 explains *why* it is load-bearing for correctness.

---

## 1. The central thesis: separate planes by their consistency requirement

The single most important design decision is to **split the system into two planes that have
fundamentally different consistency needs**, and to use the right tool for each.

| Plane | Question it answers | Consistency model | Mechanism |
|---|---|---|---|
| **Coordination plane** | Who is in the cluster? Who is alive/suspect/dead? What is each node's phase? Which node *should* own pipeline X? Has config advanced? | **AP** — eventually consistent, fast-converging | Gossip / SWIM (`memberlist`) + a ring **derived** from membership |
| **Mutual-exclusion plane** | Who is allowed to *actually run* pipeline X right now? | **CP** — linearizable | **Lease in cloud object storage** via conditional writes (compare-and-swap) |

The reason this split is mandatory, and not a stylistic choice, is a direct consequence of the CAP
theorem applied to requirement #4.

Gossip is an **eventually-consistent** membership protocol. During the convergence window after a
membership change — and, worse, during a network partition — two nodes can each hold a *locally
consistent but globally divergent* view of the ring, and each can conclude that it owns pipeline X.
A protocol that is available under partition (AP) **cannot** also provide the linearizable mutual
exclusion that "exactly one writer" requires (CP). No amount of tuning `memberlist` timers removes
this; it sharpens the convergence window but never eliminates the partition case.

Therefore:

> **The ring decides who *should* own a pipeline (scheduling / load distribution). A lease decides
> who *may* run it (safety / mutual exclusion). The ring is an optimization for even distribution
> and fast handoff; the lease is the correctness guarantee. They are complementary, not redundant.**

Everything below follows from this separation. The coordination plane can be cheap, fast, and
decentralized precisely because it is *not* trusted for safety; the safety property is anchored in a
single linearizable arbiter (object storage) that every node already depends on as its sink.

A pleasant corollary, expanded in §7.6: because the arbiter is object storage — which both sides of
a *gossip* partition can usually still reach — single-owner safety survives a gossip partition even
though the ring view diverges. Anchoring safety in the shared sink rather than in gossip quorum is
what makes that work.

```
                         ┌─────────────────────────────────────────────┐
                         │            COORDINATION PLANE (AP)            │
   SWIM probes  ───────► │  memberlist: alive/suspect/dead + NodeMeta   │
   (UDP/TCP/QUIC)        │  → every node derives the SAME hash ring     │
                         │  → ring says: "node N should own pipeline X" │
                         └──────────────────────┬──────────────────────┘
                                                │  (advisory: schedule)
                                                ▼
                         ┌─────────────────────────────────────────────┐
                         │         MUTUAL-EXCLUSION PLANE (CP)           │
   Object storage  ◄───► │  leases/<pipeline_id>.json  (conditional PUT) │
   (S3/GCS/Azure)        │  → "node N may RUN pipeline X" iff it holds   │
                         │    a valid, unexpired lease                   │
                         └──────────────────────┬──────────────────────┘
                                                │  (authoritative: run)
                                                ▼
                                  Iceberg commit (atomic CAS = final fence)
```

---

## 2. What `memberlist` does and does not give us

Grounding the design in the actual library (the Rust `memberlist` crate, `al8n/memberlist`, a port
of HashiCorp's Go `memberlist`):

**It provides:**

- SWIM failure detection: periodic randomized direct probes, **indirect probes** through *k* relays
  when a direct probe fails (tolerates a single bad link / partial partition), and a **suspicion**
  sub-protocol so a node is marked `suspect` before `dead` to suppress false positives.
- Node states `alive → suspect → dead`, with **incarnation numbers** so a node can refute a false
  suspicion and so stale state cannot overwrite fresh state.
- **Lifeguard** extensions (self-awareness / dynamic suspicion timeouts) that further cut false
  positives when a node is CPU-starved or the network is laggy — directly relevant to a heavy-load
  CDC worker that may briefly stall under back-pressure.
- A `Delegate` hook (mirroring Go's `Delegate`; the Rust crate exposes it as composable
  sub-delegates via `CompositeDelegate`) that lets us piggyback application state on the gossip
  layer through three channels:
  - **Node metadata** — a small per-node blob gossiped with every alive/join message;
  - **User broadcasts** — best-effort event dissemination piggybacked on gossip
    (`get_broadcasts` / `notify_message`);
  - **Push/pull local-state ↔ merge-remote-state** — periodic full-state TCP anti-entropy sync
    (`push_pull_interval`, default 30s) that repairs anything the lossy UDP gossip dropped.
- Pluggable transport (TCP / TLS / QUIC) and runtime-agnostic async (we use tokio). TLS for the
  gossip transport is already wired in config (`globals.cluster.memberlist.tls`).

**It does NOT provide** (and must not be expected to):

- Any linearizable agreement. It is "Scalable **Weakly-consistent**…" by name. → §7 lease.
- A built-in hash ring. → §3, we derive one.
- A built-in distributed lock or leader election. → §7 lease.
- Durable storage of state — gossiped state lives only in members' memory. → durable truth lives in
  `globals.state`; gossip is the *notification + fast-path cache*, not the source of truth.

The practical consequence: **keep what we put into gossip small and derivable.** Node metadata has a
tight size budget (low-hundreds of bytes in the Go lineage). We gossip *membership and a few small
per-node fields*; we do **not** gossip the full ring or the full config (see §3.1 and §9.1).

---

## 3. Plane 1a — Membership, phases, and the derived hash ring

### 3.1 Design choice: derive the ring from membership; never gossip the ring as mutable state

There are two ways to maintain a shared hash ring across a gossip cluster:

- **(A) Gossip the ring itself** as a replicated key-value structure (each node publishes its token
  positions; nodes CRDT-merge a shared map). This is what a generic "KV store over memberlist"
  (à la Grafana Loki/Mimir) does. It is flexible but introduces merge/conflict logic and a larger
  gossip payload.
- **(B) Gossip only membership; derive the ring deterministically.** Each node *N* contributes
  `tokens` virtual nodes whose ring positions are `hash(node_id ‖ vnode_index)` for
  `vnode_index ∈ [0, tokens)`. Since every node observes the same membership set (eventually) and
  applies the same hash function and the same `tokens` count, **every node independently computes
  byte-identical rings.** This is the Cassandra / Dynamo / Loki-ingester approach.

**Recommendation: (B).** It is simpler, has a tiny gossip footprint (membership only), needs no CRDT
merge, and makes the ring a *pure function of membership* — which is exactly the property that makes
reasoning about handoff tractable. The cost is that the ring cannot encode per-node weights beyond
"token count," which we do not need (all workers are symmetric).

This is why **`tokens` must be identical cluster-wide (G-36)**: option (B)'s "every node computes the
same ring" guarantee holds *only if* every node uses the same `tokens`. If node A uses 128 and node
B uses 256, they build different rings, disagree on ownership, and manufacture a permanent split.
The config spec already enforces this with the `globals.state` anchor and refuse-to-start-on-mismatch
(V-026 / V-062). §3.3 keeps it visible because it is a safety invariant, not a tuning knob.

### 3.2 Per-node gossiped state (NodeMeta schema)

Gossiped via the `Delegate` node-metadata channel — small, slowly-changing, derivable:

```jsonc
// NodeMeta — kept under the gossip metadata size budget
{
  "v": 1,
  "node_id": "worker-7f3a9c",   // stable logical identity (see §3.4)
  "incarnation": 12,            // bumped on each PROCESS start; distinguishes a restarted pod
  "phase": "Active",            // Joining | Active | Draining | Leaving (application phase)
  "tokens": 128,                // for cross-check only; must equal the cluster anchor
  "config_generation": 87,      // highest config generation this node has applied (§9)
  "build": "1.4.2+abc123"       // for rolling-upgrade observability
}
```

Everything needed to place this node's vnodes on the ring is derivable from `node_id` + `tokens`; we
deliberately do **not** ship the 128 hashed positions in gossip. `phase` and `config_generation` are
the only genuinely dynamic fields, and both are tiny.

### 3.3 Node activity / reachability / phase — the status model

Two orthogonal status axes, kept separate on purpose:

- **Reachability/liveness (owned by SWIM):** `alive | suspect | dead | left`. We do not invent this;
  we read it from `memberlist`. `suspect` is the "probably gone but unconfirmed" state;
  `dead_node_reclaim_time` (30s, config) bounds how long a dead entry lingers before reclamation.
  `left` is the *graceful* departure marker (see §3.5) and is distinct from `dead`.
- **Application phase (owned by us, in NodeMeta):** `Joining | Active | Draining | Leaving`.
  `Joining` = process up, gossip joined, but not yet accepting pipeline ownership (still loading
  config / warming up). `Active` = eligible to own and run pipelines. `Draining` = handing pipelines
  off (planned scale-down / rebalance), still finishing in-flight work. `Leaving` = drain complete,
  about to exit.

The ring-ownership computation considers a node only if it is `alive`/`left==false` **and**
`phase ∈ {Active, Draining}` with appropriate handling (a `Draining` node keeps ownership only until
its handoffs complete). This prevents a half-initialized `Joining` node from being assigned work
before it is ready.

### 3.4 `node_id` stability and incarnation (the Quickwit lesson)

`node_id` must be **stable across reconnects** but **distinguish a fresh process**. The
Quickwit/Scuttlebutt experience (a source in the index) is explicit: node identity needs a
monotonically increasing generation so that a restarted node's *fresh* state cannot be overwritten
by its own *stale* gossip still in flight. We adopt the same pattern with the `incarnation` field:

- `node_id`: derived from a stable identity. On Kubernetes, prefer the **StatefulSet pod name** (or
  pod UID); for standalone, a persisted-once UUID or `${HOSTNAME}`. It should *not* embed an 
  ephemeral IP, because pod IPs change across restarts and we want the ring to be stable when the
  *same logical worker* restarts.
- `incarnation`: a wall-clock-seeded or counter value bumped on every process start. Used to ignore
  resurrected stale NodeMeta from a node that just restarted.

> **Decision (D-1) — resolved:** `node_id` *is* the existing `globals.cluster.memberlist.node_name`
> field (config spec §6.5.1, defaults to OS hostname) — not a new field, just a sourcing rule:
> (a) on **Kubernetes set `node_name` to the StatefulSet pod name** via the Downward API; (b) this
> **mandates a StatefulSet, not a Deployment**, for the worker workload — a Deployment's random,
> changing pod names would reshuffle the derived ring on every restart (ring stability depends on
> stable identity); (c) **standalone** uses a persisted-once UUID on a durable path, falling back to
> `${HOSTNAME}` only when there is no durable volume. `incarnation` (§3.2) still distinguishes a fresh
> process of the same `node_id`. Config-spec follow-up: document the node_name sourcing rule and the
> StatefulSet constraint in §6.5.1 (proposed).

### 3.5 Coordinated join, leave, and status update

- **Join:** seed addresses from `globals.cluster.memberlist.join_members`; SWIM join + push/pull
  brings the joiner up to date. On k8s, seeds resolve from a **headless Service** (§10.1).
- **Status update:** mutate local NodeMeta (e.g., `phase: Active → Draining`) → `memberlist`
  re-broadcasts via alive/gossip; converges in O(log N) rounds; the 30s push/pull repairs stragglers.
- **Graceful leave:** the `leave cluster` REST endpoint triggers `memberlist.leave()`, which
  broadcasts an intent so peers record `left` rather than `dead`. **This distinction is what lets us
  skip the full lease-expiry wait on planned departures (§8.3):** a `left` node has already drained
  and released its leases, so the new owner can acquire immediately; a `dead` node has not, so the
  new owner must wait out the lease (§8.2).

### 3.6 SWIM vs Scuttlebutt — decision (closes G-23)

The source index flags a documented tension: Quickwit moved SWIM → Scuttlebutt for "more reliable
state reconciliation," using a Phi-accrual failure detector and generation-tagged node IDs.

**Decision: keep SWIM via `memberlist`, with Lifeguard enabled, and adopt the *good ideas* from the
Scuttlebutt camp without switching libraries.** Rationale:

- The reasons people prefer Scuttlebutt are (i) anti-entropy reconciliation of arbitrary KV state
  and (ii) Phi-accrual's graceful failure scoring. We neutralize (i) by **not** putting arbitrary
  mutable KV in gossip at all — our ring is derived (§3.1) and our durable truth is in object storage
  (§7, §9). We mitigate (ii) by enabling **Lifeguard**, whose self-awareness/dynamic-timeout
  behavior addresses the same "is it dead or just slow?" problem that Phi-accrual targets, and — more
  importantly — by making **SWIM accuracy a latency concern, not a safety concern** (§8.4): a false
  "dead" verdict at worst triggers a redundant *lease-takeover attempt* that the lease itself
  rejects.
- `memberlist` is already the chosen, well-understood dependency (System-Overview "Cluster state =
  memberlist"); switching to a Scuttlebutt implementation adds risk for benefits we have designed
  around.
- We **borrow** the generation-tagged identity idea (§3.4 `incarnation`) regardless.

**Config action:** enable Lifeguard explicitly; record this decision in the architecture doc.

---

## 4. Plane 1b — Pipeline placement on the ring

### 4.1 The hash key is the pipeline `id`, not its name (closes OQ-P3)

Placement: `ring_owner(pipeline) = owner of the first vnode clockwise from hash(pipeline.id)`.

Use the **immutable `id`** (config spec §9: `id` is the immutable identity, charset-constrained,
unique; a *rename* changes only the display key and the `_pipeline` label, not `id`). Hashing `id`
means a rename never remaps a pipeline to a different worker, while an `id` change is — correctly — a
delete-and-recreate that *does* move it. This matches the config spec's reconcile semantics (§12.3)
exactly.

### 4.2 Even distribution and the small-N caveat

128 vnodes per node gives good balance: the load standard deviation across nodes falls roughly as
`1/sqrt(total_vnodes)`. With even a handful of nodes that is hundreds–thousands of vnodes, so the
*ring* is well balanced.

**Caveat worth surfacing:** balance of *pipelines* also depends on the number of pipelines. With
`P` pipelines and `M` total vnodes, when `P ≫ nodes` the assignment is effectively uniform; when `P`
is small (say 3 pipelines on 4 nodes) the outcome is lumpy regardless of vnode count, because each
pipeline is a single indivisible unit landing independently. This is inherent to hashing
indivisible units, not a flaw in the ring. If operators run few, heavy pipelines and want tighter
balance, see the alternative below.

### 4.3 Alternative placement function — Rendezvous (HRW) hashing

**Alternative B (offer, not default):** Rendezvous / Highest-Random-Weight hashing. For each
pipeline, compute `score(pipeline.id, node_id)` for every live node and assign to the highest score.

- Pros: no token bookkeeping at all; provably minimal disruption on membership change (only the
  pipelines whose top-scoring node left/joined move); slightly better small-`P` balance than vnode
  hashing; trivially supports per-node weights if ever needed.
- Cons: assignment is `O(pipelines × nodes)` per recompute (negligible at our scale — dozens of
  nodes, hundreds of pipelines), and it is a different mental model from the "token ring" the brief
  and config (`tokens`) already assume.

**Recommendation:** keep **vnode consistent hashing as the primary** (it matches the existing
`tokens` config, the brief's "ring of tokens" framing, and the `hashring` crate already in the
source index). Note HRW as a drop-in alternative for the placement function if small-`P` balance
becomes a real operational complaint — the rest of the architecture (lease, drain, propagation) is
identical either way, because they all consume an *owner(pipeline) → node* function and do not care
how it is computed.

### 4.4 Smooth transfer property

Both functions have the consistent-hashing virtue: a membership change of one node moves only
`≈ P/N` pipelines, not the whole assignment. Combined with the lease + drain protocol (§8), the
movement is not just *small* but also *safe and graceful*.

---

## 5. Plane 2 — The lease: the exclusive single-runner guarantee (closes G-8)

This is the core of the design and the resolution of the system's "single most critical safety gap."

### 5.1 Why the ring alone cannot provide it (restated concretely)

Scenario: a 1s GC pause on node A delays its SWIM probes; B's suspicion fires; B marks A `dead`; B
recomputes the ring and concludes it now owns pipeline X; A, unaware, is still streaming X and
committing to Iceberg. For a window, **both** A and B write X. Result without a lease: duplicate
Iceberg commits, doubled equality-deletes, possibly corrupted MOR state, and — for MySQL — two
binlog readers racing. The ring is *working as designed*; it simply is not an exclusion primitive.

(One natural partial mutex exists and is worth knowing: a **PostgreSQL replication slot allows only
one active consumer**, so two PG workers on the same slot — the second errors out. That protects the
*PG source side* but not the *sink side* and not MySQL binlog, which permits multiple readers. So a
lease is still required for sink correctness and for MySQL. Do not rely on the PG slot as the
exclusion mechanism; treat it as a helpful backstop only.)

### 5.2 The lease: a CAS object in `globals.state`

We use a **cloud-storage lease** built on the **conditional-write (compare-and-swap) primitives that
all three target stores now support**: S3 added create-if-absent (`If-None-Match: *`) in Aug 2024 and
ETag compare-and-swap (`If-Match: <etag>`) in Nov 2024; GCS (generation-match) and Azure Blob
(ETag/If-Match) have had CAS for years. A failed precondition returns `412 Precondition Failed`
(occasionally `409` on S3, which is a retry signal). This is the same primitive the data community
uses for S3 leader election and that Delta Lake uses for commit coordination.

Why storage-lease rather than an external lock service: the system already mandates "no local state;
everything durable goes to cloud storage," already depends on object storage as its sink, and
deliberately avoids extra infrastructure. The lease should live where the rest of the truth lives.
`globals.state` already reserves a home for the "lease registry."

**Lease object** — `…/leases/<pipeline_id>.json`; the object's **ETag/generation is the real CAS
handle**, the JSON timestamps are advisory only:

```jsonc
{
  "version": 1,
  "pipeline_id": "orders",
  "holder_node_id": "worker-7f3a9c",
  "holder_incarnation": 12,
  "owner_epoch": 87,            // monotonic; +1 on every successful acquisition (app-layer fence, §5.5)
  "acquired_at": "2026-06-14T10:00:00Z",   // holder clock — informational
  "renewed_at":  "2026-06-14T10:00:10Z",   // holder clock — informational
  "lease_duration_ms": 15000
}
```

### 5.3 Clock-skew-safe acquire / renew / takeover protocol

The protocol never compares one node's clock to another's. The holder's liveness is judged by
**whether the object's ETag changes within a locally-measured interval**, which is immune to clock
skew. The only assumption is single-node: *a live holder renews at least once per `lease_duration`.*

**Acquire (no holder / object absent):**
`PUT leases/<id>.json` with `If-None-Match: *`. `200/201` → acquired (`owner_epoch = 1`). `412` →
someone holds it → go to *takeover-evaluation*.

**Renew (current holder), every `lease_renewal_interval`:**
`PUT … If-Match: <my_last_etag>` with updated `renewed_at`; store the **new** ETag. A `412` means a
challenger took over → **I have lost the lease**: immediately **stop the pipeline and do not commit.**
But a *renew that fails to reach storage* (timeout / `5xx` / throttle / conn-reset) is **not** the
same as a `412` and must **not** trigger an immediate stop — it is retried within a bounded safety
deadline. The full failure taxonomy, the safety-deadline formula, and the renewer state machine are
specified in **§19 (D-8)**; the one-line summary is: `412`/`404`/`403` → stop now; transient → retry
until success or `last_renew_local + lease_duration − guard`, then stop.

**Takeover-evaluation (challenger that became ring-owner while another node holds the lease):**
1. `GET` the lease → record `etag0`; start a **local** timer.
2. Wait `lease_duration` of local elapsed time (this wait may overlap SWIM detection — start it the
   moment the ring recompute makes you the owner).
3. `GET` again. If `etag ≠ etag0` → the holder renewed → it is alive → **abandon** the takeover (you
   should not own it; back off and re-evaluate).
4. If `etag = etag0` after a full `lease_duration` → holder presumed dead → `PUT … If-Match: <etag0>`
   with yourself as holder and `owner_epoch += 1`. `200` → you are the new owner. `412` → another
   challenger beat you; re-read and reconcile.

Because the challenger measures elapsed time on its own clock and the CAS rejects any write if the
holder renewed (ETag moved), **two nodes can never both win.** Storage is the arbiter.

### 5.4 The two halves of requirement #4

- **At most one (safety):** the lease + the "renew-or-stop" rule + the pre-commit re-check (§5.5).
- **At least one (liveness):** the **reconciliation loop** (§8.1). Because the ring covers the whole
  keyspace, *every* pipeline always has exactly one ring-owner among live nodes; that owner's
  reconcile loop acquires the lease and starts the pipeline. A pipeline can be momentarily unowned
  only during the lease-expiry-to-reacquire window after an *unplanned* death, bounded by
  `lease_duration + takeover_latency` (≈ a `GET`, the wait, a `PUT` ≈ one `lease_duration` + ~1–2s).
  That window is the safety/liveness tuning knob.

### 5.5 The fencing gap and Iceberg as the final fence (honest limitation)

S3 conditional writes **do not provide a monotonic fencing token** (Azure/GCS expose ETag/generation
on the *lease object* but there is no storage-level fence carried into *unrelated* writes). So a
"zombie" holder — GC-paused past expiry, lease already taken by a new owner — could in principle
attempt one stale Iceberg commit before noticing. We close this with **defense in depth**:

1. **Pre-commit lease check:** the owner re-validates the lease (cheap `GET` / piggybacked on the
   renew cadence) **immediately before every Iceberg commit**; a zombie's check fails (`renewed_at`
   moved / its renew returns `412`) and it aborts.
2. **Iceberg atomic commit (optimistic concurrency) is the last line of defense:** Iceberg commits
   are a CAS on the table-metadata pointer (already in `Data-Flow.md`'s "catalog conflict → reload &
   retry" path). If a zombie slips past (1), the new owner has already advanced the metadata, so the
   zombie's commit conflicts and fails. The new owner never loses; at worst the zombie wastes effort.
3. **`owner_epoch` as a sink-side fencing token (D-7 — resolved: assert it).** The monotonic
   `owner_epoch` is **embedded in the Iceberg snapshot summary** (`cdc.owner_epoch`), and the commit
   path — inside the existing catalog-conflict reload-retry loop (Data-Flow.md) — **rejects a commit
   when `latest_committed_epoch > my_epoch`** (a higher epoch already owns ⇒ I am the zombie, abort).
   This upgrades `owner_epoch` from advisory attribution to a real **fencing token at the sink**:
   it stops a zombie from committing *after* a new owner has established ownership. It does not undo
   the rarer "zombie commits in the gap *before* the new owner's first commit" — that single stale
   commit still lands and is caught by (4) `_lsn` dedup. This is defense-in-depth layered on top of
   the metadata-pointer CAS (2), not a correctness prerequisite; cost is low because iceberg-rust
   exposes the snapshot-summary string map and the monotonic check rides the reload-retry path that
   already exists. **Implementation note:** verify the chosen iceberg-rust version preserves custom
   snapshot-summary properties across commits.
4. **Idempotent/dedup writes** (the `_lsn`-based exactly-once guard, Phase 4, and the ghost-row
   dedup gap G-13) absorb the residual duplicate-row possibility.

> **Be explicit in design reviews:** perfect exactly-once under *arbitrary* process pauses is not
> achievable from a storage lease alone, because the storage offers no fencing token. The lease makes
> it *correct in the overwhelming common case and cheap*; **Iceberg's atomic commit is what makes it
> *safe in the worst case*.** This is precisely the "lease is primary, Iceberg commit is last line of
> defense" stance the gap analysis already recommended (4.1).

### 5.6 Alternatives considered

| Option | Verdict | Why |
|---|---|---|
| **Cloud-storage lease (CAS)** | **Recommended** | Portable across S3/GCS/Azure and across k8s/container/Linux-service; no extra infra; lives where the sink lives; arbiter survives gossip partitions (§7.6). |
| **Kubernetes `Lease` API + leader-election** | Optional optimization on k8s | Battle-tested (client-go pattern), sub-second failover, native fencing-ish via `resourceVersion`. **But k8s-only** — fails the standalone-container / Linux-service requirement. Offer as an *opt-in discovery/lock backend* when running on k8s; storage lease remains the portable default. |
| **etcd / Consul session locks** | Rejected for core | Strong and well-understood, but adds a CP infrastructure dependency the system explicitly avoids ("operational simplicity," no extra state stores). |
| **Iceberg optimistic concurrency only (no lease)** | Rejected as *primary* | Prevents *corruption* but not *wasted duplicate work*, not MySQL double-reads, not flapping; gives no "who should start it" liveness story. Kept as the §5.5 backstop, not the mechanism. |

> **Decision (D-2) — resolved:** cloud-storage CAS lease is the **primary/default** backend.
> (a) Reserve `cluster.lock_backend: storage | kubernetes` but **build `storage` only for v1** — the
> `kubernetes` (K8s Lease API) backend is a future `LeaseStore` impl; do not build two backends before
> the first ships. (b) `lock_backend` is **cluster-wide-consistent and pinned in the `globals.state`
> anchor** alongside `tokens`/`lease_duration` (§12) — a cluster running *mixed* backends is
> split-brain by construction (two nodes using different arbiters can both "win"), so a node whose
> `lock_backend` differs from the anchor **refuses to start**. Config-spec follow-up: add the
> `lock_backend` enum and its anchor/consistency rule (proposed).

---

## 6. (reserved — merged into §5 and §8)

---

## 7. Cross-cutting correctness: partitions, clocks, pauses, and the arbiter

### 7.1 Network partition (gossip split)
Gossip partitions into {A,B} | {C}. Both sides recompute *different* rings and may both believe they
own pipeline X. **Safety holds anyway:** the lease object lives in object storage, which both sides
usually still reach; only one side wins the CAS. The losing side's owner cannot renew/acquire and so
does not run X. The cost is *availability of some pipelines on one side*, not *correctness* — the
correct trade for a CDC sink. This is the decisive advantage of anchoring exclusion in shared storage
rather than in gossip quorum.

### 7.2 Object storage unreachable
If S3/GCS/Azure is unreachable, the pipeline cannot commit (the store is the sink) — so it cannot make
progress regardless of leasing. Lease unavailability is **correlated** with sink unavailability and
fails safe (no progress, no split). No special handling beyond the existing Iceberg-write-failure
back-off path.

### 7.3 Clock skew
Eliminated from the safety path by §5.3 (locally-measured intervals + ETag CAS). Wall-clock fields in
the lease are advisory/observability only. **`lease_duration` must be cluster-wide consistent** (a
shorter value on a challenger could shorten its wait and race a slower holder) — so it is anchored
like `tokens` (§11) and treated as PROC.

### 7.4 GC / stop-the-world pauses
Covered by §5.5: renew-or-stop + pre-commit check + Iceberg CAS. Set `lease_duration` comfortably
above the worst expected pause (heavy back-pressure stalls included); Lifeguard reduces the chance
that such a pause is even *misread* as death.

### 7.5 Replication-slot / WAL safety during failover
When a PG owner dies, its slot is left active-less; the new owner reconnects and resumes from the
slot's `confirmed_flush_lsn` (Data-Flow.md). The abandoned-slot **WAL-accumulation** risk on the
source is a *separate* gap (G-22, `max_wal_lag_bytes`) — out of scope here but noted so the failover
story is honest: fast lease takeover also bounds how long a slot sits unconsumed.

### 7.6 Why this is more robust than a gossip-quorum lease
A pure-gossip "majority owns it" scheme would lose availability for the *minority* side of a partition
*and* still need a tiebreak for the convergence window. Anchoring in storage CAS gives a single,
always-consistent arbiter without standing up a quorum system, and degrades to exactly the right
behavior under partition.

---

## 8. Failure detection → reassignment → rebalancing (requirement 3, closes G-9)

### 8.1 The reconciliation loop (the heartbeat of liveness)
Every node runs a periodic **reconcile** (a tokio task; suggested cadence ≈ `lease_renewal_interval`):

```
for each pipeline P in the full config:
    owner := ring_owner(P)            // pure function of current membership
    if owner == self:
        ensure_lease_held(P)          // acquire (§5.3) or takeover-evaluate if held by a dead node
        ensure_running(P)             // start Phase 0 if not running
    else:
        if running_locally(P):        // ring moved P away from me
            begin_graceful_handoff(P) // §8.3
        // else: not mine, not running — nothing to do
```

This loop is what guarantees **every pipeline is running** (some node is always the owner and will
start it) and **only one runs it** (the lease gate inside `ensure_lease_held`).

> **Execution refinement (§20.1, D-9):** this pseudocode describes the *desired state* correctly, but
> it must **not** be executed as one loop with inline blocking calls — `ensure_lease_held` can wait a
> full `lease_duration` during takeover-evaluation (§5.3) and would stall every other pipeline behind
> it. In the implementation it is split into a **`ClusterReconciler`** (owns the ring; pushes per-
> pipeline `Own`/`DontOwn` targets; never touches a lease) and a **per-pipeline `PipelineController`**
> (sole lease-writer; runs acquire/takeover/renew/handoff in its own task). Triggering is event-driven
> with a periodic resync backstop, and controllers are idempotent/level-triggered. See §20.1.

### 8.2 Unplanned death path (timing)
```
Node A dies
│
├─► SWIM: missed direct probe → indirect probes → suspect → dead
│     (≈ probe_interval + suspicion; Lifeguard tunes this)
├─► Gossip propagates "A dead"; every node recomputes the ring (membership changed)
│
└─► New ring-owner B for each of A's pipelines runs takeover-evaluation (§5.3):
      └─► waits one lease_duration of UNCHANGED lease ETag, then CAS-acquires
      └─► starts pipeline (Phase 0): reads state from cloud, resumes from
          confirmed_flush_lsn / gtid_executed  (worst-case replay = since last checkpoint)
```
**End-to-end failover ≈ SWIM-detect + lease_duration + acquire-latency.** With defaults
(probe 1s / suspicion + `lease_duration` 15s) that is on the order of ~15–20s. Shorten `lease_duration`
for faster failover at the cost of more renewal traffic and higher sensitivity to transient storage
latency.

### 8.3 Planned drain / handoff protocol (the missing G-9 piece)
Triggered by (a) the `leave cluster` endpoint / SIGTERM (scale-down), or (b) a rebalance decision when
a new node joins and the ring moves a pipeline away from its current owner. The departing/yielding
owner runs:

```
DRAIN(P):
  1. phase → Draining (gossiped); stop accepting new source events for P
     (PG: pause reader; MySQL: pause binlog consumption)
  2. flush buffered Arrow RecordBatches → Iceberg commit (atomic)
  3. write final confirmed_flush_lsn / gtid_executed + snapshot cursors to state storage
  4. RELEASE the lease:  PUT a tombstone / DELETE leases/<id>.json  with If-Match:<my_etag>
     (a released lease lets the new owner acquire IMMEDIATELY — no lease_duration wait)
  5. broadcast departure (already implied by memberlist.leave() for whole-node leave)
The new owner's reconcile loop sees the released lease and acquires without waiting.
```
This mirrors the existing `Data-Flow.md` shutdown sequence (reader → accumulator flush → writer →
final checkpoint) and extends it with the explicit **lease release** so planned handoff is fast and
clean, versus the at-least-once *replay* of the unplanned path.

- **Bound it:** `cluster.drain_timeout` (default 60s, matching the gap-analysis recommendation). If
  drain exceeds the timeout, fall back to the unplanned path (stop, let the lease expire, new owner
  replays from last checkpoint). Never block shutdown indefinitely.

### 8.4 SWIM accuracy is a *latency* concern, not a *safety* concern
A SWIM **false positive** (A marked dead but actually alive) makes B start takeover-evaluation, but
A keeps renewing, so the lease ETag keeps moving, so B's CAS in step 4 of §5.3 never fires → B backs
off. No split-brain; the only cost is a wasted `GET` and a delay. This is why we can run SWIM
relatively *aggressively* for fast detection without endangering correctness — the lease is the
safety net. It is also why we did not need Phi-accrual (§3.6).

### 8.5 Anti-flap and rebalance rate-limiting
Naive "recompute ring on every membership event and immediately rebalance" causes thundering-herd
handoffs during a rolling deploy (every pod restart reshuffles the ring). Mitigations:

- **Stabilization delay** (`cluster.rebalance_stabilization`, default 15s): do not act on a
  membership change for rebalancing purposes until membership has been *stable* for this long.
  Failover for *dead* nodes is **not** delayed (that path is driven by lease expiry, which is its own
  timer); only *proactive rebalancing on joins* waits for stability.
- **Bounded concurrent handoffs** (`cluster.max_concurrent_handoffs`, default 2): drain at most *k*
  pipelines at once, like Akka Cluster Sharding's "rebalance N shards at a time," to cap source-DB
  reconnect storms and flush bursts.
- **Hysteresis:** a node that just *joined* enters `Joining`, warms up, then `Active`; ownership is
  only granted once `Active`, so a flapping joiner never repeatedly grabs and drops pipelines.

---

## 9. Configuration "in the ring" and propagation (requirement 5, closes G-2 / G-17, OQ-API3)

### 9.1 Storage is the source of truth; gossip is the notification
Do **not** put the full config into gossip (it can be large; gossip is for small deltas). Instead:

- **Durable truth in `globals.state`:** a monotonic **generation pointer** plus an immutable
  per-generation resolved-config snapshot:
  ```
  config/generation.json          { "version":1, "generation": 87, "hash": "sha256:…",
                                     "written_by": "worker-7f3a9c", "written_at": "…" }
  config/snapshots/000087.yaml     # the resolved config for generation 87 (immutable)
  ```
- **Tiny signal in gossip:** each node's NodeMeta carries `config_generation` (the highest it has
  *applied*). That is the entire cross-node config footprint in gossip.

This is the "every config change is an entry in a log" pattern (cf. the Restate "every system is a
log" source): generation N is durable and immutable; nodes converge by replaying up to the latest.

### 9.2 Reload flow (no proxying — closes OQ-API3 / 9.3)
```
1. Operator → POST /v1/config/reload  on ANY worker A.
2. A re-reads its file → strict parse → validate → resolve (§11 of the config spec).
3. A computes new generation = current_generation + 1, writes:
     config/snapshots/<N>.yaml,
     then CAS-updates config/generation.json with If-Match:<etag of generation N-1>.
   (CAS serializes concurrent reloads: a racing reload on B gets 412 and retries against N.)
4. A applies N locally (reconcile §12), bumps its NodeMeta config_generation → gossiped.
5. Every other worker observes target generation advanced (via the gossiped config_generation of
   peers, or a short poll of generation.json), FETCHES config/snapshots/<N>.yaml, runs its OWN
   reconcile over ITS OWN owned pipelines, and bumps its config_generation.
6. A returns 202 Accepted immediately; GET /v1/config/status reports per-node applied generation;
   the reload is "complete" when all live nodes report N.
```

**There is no reload proxy and no per-pipeline routing of the reload.** Every node holds the *full*
config anyway (it must, to compute the ring and to be ready to take over any pipeline), so a node
that owns pipeline X simply reconciles X when it adopts generation N. Worker A does not forward the
request to the owner of X; it publishes the generation and every owner reconciles itself. This is
strictly simpler than proxy/redirect/broadcast-of-the-payload and avoids the "who is the owner *right
now*" race during a reload.

### 9.3 Cold-start precedence (the k8s ConfigMap-lag footgun)
A node's **mounted file is its input**; the **cloud generation pointer is the distributed runtime
truth.** On start, a node loads its file, derives a candidate config, then checks
`config/generation.json`:

- **If `config/generation.json` exists → adopt the cloud snapshot** for runtime (and log the
  divergence vs. the local file). This prevents a freshly-started pod whose ConfigMap has not yet
  updated from *regressing the whole cluster* to a stale config.
- The local file is authoritative **only at first bootstrap** (no `generation.json` yet → the starting
  node CAS-creates generation 1 from its file; concurrent starters race on `If-None-Match:*`, one
  wins, the rest adopt) **or via an explicit `reload`** (which promotes the now-updated file to
  generation N+1 in the normal way).

> **Decision (D-3) — resolved:** cloud-wins-at-runtime, file-is-input, reload-promotes.
> (a) **Simplified rule:** there is no "compare the file to a generation number" step — a file carries
> no generation; a starting node simply adopts the latest cloud generation if one exists, and the file
> matters only at first bootstrap or on explicit reload. (b) **Operator footgun, documented loudly:**
> **a pod restart does NOT apply config-file changes — only `POST /v1/config/reload` does.** Under
> cloud-wins this surprises operators who edit a ConfigMap and restart expecting it to take effect.

### 9.4 Mutability interaction
The generation mechanism is orthogonal to the R/PR/PROC classes: once a node adopts generation N it
runs the existing §12 reconcile, which already maps each changed field to R (apply in place) / PR
(restart pipeline) / PROC (reject, needs process restart). PROC changes that cannot be hot-applied
are rejected at validation time on worker A *before* a generation is ever published, so the cluster
never adopts a generation that some nodes cannot apply.

---

## 10. Single-node mode and seamless scale-out (requirement 6, closes OQ-CL4 / 4.4)

### 10.1 Single-node = a one-member cluster (no special case)
Empty `join_members` and no discovered peers ⇒ **single-node mode**, and crucially **the same code
path**:

- Ring contains only self's vnodes ⇒ self owns every pipeline.
- Gossip runs but is idle (no peers to probe) — no special "disable gossip" branch.
- **The lease stays active.** Cost is one periodic conditional PUT per pipeline — negligible — and it
  guards the real-world k8s hazard of *two singletons briefly coexisting* (rolling deploy where the
  old pod has not terminated before the new one starts, both pointed at the same `globals.state`
  bucket). The lease makes "accidentally two single-node instances" safe by construction.

There is **no `cluster.enabled` toggle**; single vs multi is determined purely by whether peers
exist. This is what "single-node cluster always ready to attach a node" means operationally.

### 10.2 Peer discovery for scale-out (k8s)
To go from one node to many with zero reconfiguration, prefer **DNS-based discovery** over a static
seed list:

- **Headless Service** (`clusterIP: None`) over the worker StatefulSet ⇒ the Service DNS name
  resolves (A/AAAA, or SRV for ports) to *all* pod IPs. `memberlist` joins against that name and
  discovers all current peers.
- New pod starts → resolves the headless Service → joins gossip → membership grows → every node
  recomputes the ring → some pipelines move from node 1 to the newcomer via the **normal graceful
  handoff (§8.3)** → newcomer acquires their leases and resumes. No promotion step, no leader, no
  manual rebalance.

> **Decision (D-4) — resolved:** add `cluster.discovery: static | dns` (default `static`) and
> `cluster.discovery_dns_name` so the seed mechanism is config-driven. `static` keeps `join_members`;
> `dns` resolves a headless Service to all pod IPs. (a) **`dns` mode re-resolves the headless Service
> periodically**, not just at boot, so a node that has lost all peers can re-bootstrap; once gossip is
> joined, SWIM carries membership and DNS is only the bootstrap/rejoin path. These fields are already
> reserved in config spec §16 (G-9 row).

### 10.3 The N=1 ↔ N>1 symmetry
The architecture is *identical* at every cluster size; only the membership set differs. That symmetry
is the property the brief asks for ("ready to transform into a real multi-node cluster at any
moment") and it falls out for free from "ring derived from membership + lease for exclusion + reconcile
loop."

---

## 11. Recommended configuration additions

These extend the `globals.cluster` block. They populate the slots the config spec §16 already
reserved for G-8, plus the new G-9 / rebalancing fields. Mutability classes follow the spec's R/PR/PROC
model. **Timing fields that affect the safety of takeover are cluster-wide-consistent and PROC,
anchored like `tokens` (G-36).**

| Field | Type | Default | Mutability | Cluster-wide consistent? | Notes |
|---|---|---|---|---|---|
| `cluster.lease_duration` | Duration | `15s` | PROC | **Yes (anchored)** | Lease TTL = the safety boundary and the worst-case unowned window after unplanned death. A shorter value on one node could race a slower holder, so it must match cluster-wide (§7.3). |
| `cluster.lease_renewal_interval` | Duration | `5s` | PROC | Recommended equal | Must be ≤ `lease_duration / 3` (tolerate 2 lost renewals). Local liveness property. |
| `cluster.lock_backend` | enum | `storage` | PROC | Yes | `storage` (CAS lease, default/portable) or `kubernetes` (k8s Lease API, opt-in on k8s). §5.6. |
| `cluster.rebalance_stabilization` | Duration | `15s` | R | No | Delay proactive rebalancing until membership stable this long. Does **not** delay dead-node failover. §8.5. |
| `cluster.max_concurrent_handoffs` | integer | `2` | R | No | Cap simultaneous graceful drains. §8.5. |
| `cluster.max_concurrent_takeovers` | integer | `4` | R | No | Cap simultaneous Phase-0 starts on the **unplanned**-takeover path (gates the resume, not the cheap takeover-evaluation wait); de-synchronized with jitter. §20.2. |
| `cluster.drain_timeout` | Duration | `60s` | R | No | Max graceful drain before falling back to unplanned replay. §8.3. |
| `cluster.discovery` | enum | `static` | PROC | No | `static` (use `join_members`) or `dns` (resolve a headless Service). §10.2. |
| `cluster.discovery_dns_name` | string | — | PROC | No | Required when `discovery: dns`. |
| `cluster.memberlist.lifeguard_enabled` | bool | `true` | PROC | Recommended equal | Enable Lifeguard (G-23 decision). |
| `cluster.lease_safety_margin` | float (0–0.5) | `0.15` | PROC | Recommended equal | Fraction of `lease_duration` held back as `guard` before the renewal safety deadline (§19.3). Also: `lease_renewal_interval ≤ lease_duration / 3` is an **enforced** rule (§19.6), not just advisory. |

> The `lease_duration` / `lease_renewal_interval` pair are the two fields config spec §16 already
> earmarked for G-8; the rest are the G-9 / rebalancing surface. Anchoring `lease_duration` in
> `globals.state` alongside the `tokens` anchor (next section) extends the existing G-36 mechanism.

---

## 12. `globals.state` object layout (closes gap 5.1 / 5.2)

The concrete file set under `<globals.state.bucket>/<globals.state.prefix>/`. Every file carries a
top-level `version` integer for forward compatibility (the other half of gap 5.1). All mutations use
conditional writes (gap 5.2): create via `If-None-Match: *`, update via `If-Match: <etag>`.

```
<globals.state.prefix>/
├── cluster/
│   ├── anchor.json            # FIRST-WRITER-WINS via If-None-Match:*. Establishes the cluster-wide
│   │                          # invariants: { version, tokens: 128, lease_duration_ms: 15000,
│   │                          # lock_backend: "storage", hash_algo: "xxh3",
│   │                          # established_by, established_at }.   (lock_backend pinned per D-2)
│   │                          # A node whose config disagrees REFUSES TO START (extends V-026/V-062
│   │                          # from tokens-only to the full safety-timing anchor).
│   └── membership.json        # OPTIONAL advisory snapshot of last-known membership, for cold-start
│                              # hints and operator visibility. NOT authoritative (gossip is). Best-effort.
├── config/
│   ├── generation.json        # CAS pointer: { version, generation, hash, written_by, written_at }
│   └── snapshots/
│       └── <NNNNNN>.yaml       # immutable resolved config per generation
└── leases/
    └── <pipeline_id>.json      # per-pipeline lease (§5.2), CAS via ETag
```

This respects the §6.2 boundary rule precisely: `globals.state` holds **cross-pipeline** data only
(config generation, cluster anchor, membership cache, lease registry). **Per-pipeline** offsets /
cursors / snapshot status remain under the per-pipeline `state` location (`Data-Flow.md` §state
layout). Nothing here writes per-pipeline offsets into `globals.state`.

> **Implementation checkpoint (D-5):** confirm the conditional-write path. Use the cloud SDKs
> directly (AWS/GCS/Azure) or verify the `opendal` build used by `iceberg-storage-opendal` exposes
> conditional writes (`If-None-Match` / `If-Match`) for all three backends. Leasing correctness
> depends on this primitive being genuinely atomic on each target store, so it warrants a focused
> integration test per backend (including the S3 `409 ConditionalRequestConflict` retry case).

---

## 13. Rust implementation sketch (task / module structure)

```
ClusterManager (one per worker)
├── memberlist::Memberlist<Transport, Delegate>     // SWIM membership + failure detection
│   └── CdcDelegate (CompositeDelegate)
│       ├── NodeDelegate     → publishes/reads NodeMeta (§3.2); push/pull merge
│       └── EventDelegate    → optional broadcasts (e.g., "config gen advanced" nudge)
│
├── Ring (derived)                                  // pure fn(membership, tokens) → owner(pipeline_id)
│   └── recompute on every membership change (debounced by rebalance_stabilization for joins)
│
├── LeaseManager                                    // one renewer task per owned pipeline
│   ├── acquire / renew / takeover-evaluate (§5.3)  // conditional PUT via cloud SDK / opendal
│   └── renew-or-STOP signal → PipelineWorker
│
├── ConfigCoordinator                               // generation pointer poll + gossip nudge
│   └── on new generation → fetch snapshot → reconcile (§9)
│
└── ClusterReconciler (ring only, §20.1) + N × PipelineController (one per pipeline)
    ├── ClusterReconciler: recompute ring on membership/config change + periodic resync;
    │                      push Own/DontOwn target to each PipelineController (never touches a lease)
    └── PipelineController: sole lease-writer; state machine Unowned→Acquiring→Owned→Releasing;
                           hosts the §19 renewer in Owned; drives handoff (§8.3); Phase-0 start
                           gated by max_concurrent_takeovers on the unplanned path (§20.2)
```

> The earlier single-`Reconciler` box (one periodic loop calling `ensure_lease_held` inline) is
> refined into this two-layer form by §20.1 (D-9) to avoid head-of-line blocking during the
> takeover-evaluation wait.

- The `Reconciler` is the single place that ties ring + lease + running-set together; keeping that
  logic in one loop (rather than scattered event handlers) is what makes the at-most-one / at-least-one
  reasoning auditable.
- The per-pipeline lease **renewer** owns a `tokio::sync::watch` (or a cancellation token) that, on a
  `412`, immediately signals the corresponding `PipelineWorker` to halt before its next commit —
  wiring the "renew-or-stop" rule into the data path.

---

## 14. How this maps to the open gaps

| Gap / OQ | Status before | This document |
|---|---|---|
| **G-8** split-brain lease | 🔴 Blocker, open | **Resolved (design):** §5 storage CAS lease, clock-skew-safe protocol, Iceberg-CAS fence, config fields. |
| **G-9** rebalancing drain | 🔴 Blocker, open | **Resolved (design):** §8.3 drain protocol + §8.5 rate-limiting; unplanned vs planned paths separated. |
| **G-2 / G-17** config propagation + reload proxy | 🔴 open | **Resolved (design):** §9 generation-pointer + gossip-nudge; no proxy; 202 + status poll; cold-start precedence. |
| **G-23** SWIM vs Scuttlebutt | 🟡 open | **Decided:** §3.6 keep SWIM + Lifeguard; borrow generation-tagged identity; record decision. |
| **G-36** token-count consistency | ✅ resolved | §3.3 explains *why* it is a safety invariant; §12 extends the anchor to `lease_duration`. |
| **OQ-CL1** exclusive ownership | open | §5 (lease) + §5.5 (Iceberg fence). |
| **OQ-CL3** rebalance handoff | open | §8.3. |
| **OQ-CL4** single-node mode | partial | §10 first-class, same code path, lease stays on. |
| **OQ-API3** reload proxy | open | §9.2 — no proxy needed. |
| **OQ-P3** pipeline hash key | open | §4.1 — immutable `id`. |
| **gap 5.1 / 5.2** state layout / atomic updates | 🔴 open | §12 — concrete object set + `version` field + conditional-write mechanism. |

---

## 15. Decision points to resolve in the next elaboration pass

- **D-1** *(resolved)* `node_id` = `memberlist.node_name`; k8s → StatefulSet pod name (mandates a StatefulSet, not a Deployment); standalone → persisted UUID, `${HOSTNAME}` fallback. §3.4
- **D-2** *(resolved)* Cloud-storage CAS lease is primary; reserve `cluster.lock_backend: storage | kubernetes` but build `storage` only for v1; `lock_backend` pinned in the `globals.state` anchor (cluster-wide-consistent). §5.6 / §12
- **D-3** *(resolved)* Cold-start: cloud-generation-wins-at-runtime, file-is-input, reload-promotes; file matters only at first bootstrap or explicit reload; **restart ≠ apply config**. §9.3
- **D-4** *(resolved)* Add `cluster.discovery: static | dns` (+ `discovery_dns_name`); `dns` resolves + periodically re-resolves a headless Service. §10.2
- **D-5** *(elaborated below, §17)* Verify atomic conditional-write path on S3 *and* GCS *and* Azure (SDK vs opendal); per-backend conformance test. §12
- **D-6** *(elaborated below, §18)* Pick concrete defaults for `lease_duration` / `renewal_interval` against the target failover-time SLA (ties to OQ-OB2 replication-lag SLA). §8.2 / §11
- **D-7** *(resolved: assert it)* `owner_epoch` is embedded in the Iceberg snapshot summary (`cdc.owner_epoch`) and the commit reload-retry loop rejects a commit when `latest_committed_epoch > my_epoch` — a sink-side fencing token (defense-in-depth atop the metadata-pointer CAS + `_lsn` dedup). §5.5
- **D-8** *(elaborated below, §19; resolved)* Lease renewal failure taxonomy — distinguish `412` (definitive loss → stop) from transient storage errors (retry within a safety deadline), via a renewer state machine. Adds `cluster.lease_safety_margin` and promotes `lease_renewal_interval ≤ lease_duration / 3` to an enforced rule. §5.3 / §19
- **D-9** *(elaborated below, §20.1; resolved)* Reconciler ↔ renewer orchestration — split into `ClusterReconciler` (ring only) + per-pipeline `PipelineController` (sole lease-writer, hosts the §19 renewer); event-driven + periodic-resync, idempotent/level-triggered. §8.1 / §13 / §20.1
- **D-10** *(elaborated below, §20.2; resolved)* Unplanned-takeover storm rate-limiting — `cluster.max_concurrent_takeovers` (default 4) caps concurrent Phase-0 starts (not the cheap waits), with jitter; per-node-global now, per-source-endpoint group noted as future. §20.2
- **D-11** *(elaborated below, §20.3; resolved)* Flap immunity — suspect nodes stay in the ring (only `dead`/`left` removes); `rebalance_stabilization` applies to all ring additions incl. rejoins; removals act immediately. §8.5 / §20.3

---

## 16. One-paragraph summary

Run two planes. The **coordination plane** is `memberlist`/SWIM (with Lifeguard): it maintains
membership and `alive/suspect/dead/left` status, carries a tiny per-node metadata blob (phase,
incarnation, applied config generation), and from membership *derives* an identical consistent-hash
ring on every node — so the ring says who *should* own each pipeline (keyed by immutable `id`). The
**mutual-exclusion plane** is a per-pipeline **lease object in `globals.state`**, taken and renewed by
conditional writes (compare-and-swap), with a clock-skew-safe takeover protocol; it says who *may*
actually run a pipeline, and Iceberg's atomic commit is the final fence behind it. A per-node
**reconcile loop** aligns ring-owner, lease, and running-set, guaranteeing both *at most one* and *at
least one* runner per pipeline. Dead nodes are detected by SWIM and reclaimed by lease expiry
(SWIM accuracy affects only *latency*, never *safety*); planned departures **drain → flush → commit →
checkpoint → release lease** for fast, clean handoff, rate-limited to avoid rolling-deploy churn.
Config propagates as an immutable **generation** in object storage with a tiny gossip nudge — no
reload proxy, every node reconciles its own owned pipelines. A single node is just a one-member
cluster on the identical code path with the lease still on, and it scales out seamlessly via headless-
Service discovery. Safety is anchored in object storage, so it even survives a gossip partition.

---

## 17. D-5 elaborated — Conditional-write conformance across S3 / GCS / Azure

The entire lease rests on **one primitive: atomic compare-and-swap on a single object.** Everything
in §5 reduces to three operations, and D-5 is about confirming each is genuinely atomic on each
target store and choosing the implementation route.

| Lease op | Semantics | Maps to |
|---|---|---|
| **ACQUIRE** | create-if-absent | conditional create |
| **RENEW / TAKEOVER** | swap-if-version-matches | CAS update |
| **RELEASE** | delete (or tombstone) if-version-matches | conditional delete/write |

### 17.1 The three backends are *not* uniform — and GCS is the trap

| Backend | Create-if-absent | CAS update | Conditional delete | **Version token** | Fail code |
|---|---|---|---|---|---|
| **S3** | `If-None-Match: *` (Aug 2024) | `If-Match: <etag>` (Nov 2024) | `DeleteObject If-Match: <etag>` | **ETag** | 412 (sometimes **409 `ConditionalRequestConflict`** → retry) |
| **GCS** | `x-goog-if-generation-match: 0` | `x-goog-if-generation-match: <generation>` | delete w/ `x-goog-if-generation-match` / `…-metageneration-match` | **generation (int64)** — **not** ETag for writes | 412 |
| **Azure Blob** | `If-None-Match: *` | `If-Match: <etag>` | delete w/ `If-Match: <etag>` | **ETag** | 412 |

**The GCS trap:** GCS write preconditions use the object **generation**, not the ETag. GCS *returns*
an ETag, but ETag-match is honored only on **reads**, not on object-modifying writes. An
implementation that feeds the GCS ETag into a "write if-match" gets **no CAS** and silently breaks
single-writer on GCS. The portable abstraction must therefore treat the version token as **opaque**
and source it correctly per backend: ETag for S3/Azure, generation for GCS.

Two further caveats that apply to **all three** (not S3-only): none of them deliver a **fencing
token** into *unrelated* writes (e.g., Iceberg data files), so the §5.5 Iceberg-commit fence is
required regardless of backend; and if S3 bucket **versioning** is enabled, conditional-create still
behaves (412 on a live version) but deletes leave delete-markers — keep the lease prefix versioning-off
or lifecycle-expired to avoid clutter.

> **Versioning footgun confirmed by the conformance run (2026-06).** On a **versioning-enabled S3
> bucket** the write/stat metadata's `version()` becomes the object **version-id**, which `If-Match`
> does **not** accept — a swap CAS'd against the version-id fails `412` every time. The token fed into
> `if_match` on S3/Azure must be the **ETag** (on GCS, the generation). The `LeaseStore` fixes the token
> kind by backend, never inferring it from whichever metadata field happens to be populated. Net: bucket
> versioning is **not required** and is **best left off** for the lease prefix (the high-frequency renew
> writes would also pile up noncurrent versions).

### 17.2 Route A — OpenDAL (already a dependency via `iceberg-storage-opendal`)

OpenDAL exposes the primitives directly:

```rust
// ACQUIRE — create-if-absent via the portable if_not_exists (NOT if_none_match; see note)
op.write_with(&key, bytes).if_not_exists(true).await       // Err(ConditionNotMatch|AlreadyExists) if held
// RENEW / TAKEOVER
op.write_with(&key, bytes).if_match(&version).await         // Err(ConditionNotMatch) if version moved
// RELEASE — conditional tombstone write (OpenDAL has no conditional delete)
op.write_with(&key, TOMBSTONE).if_match(&version).await     // Err(ConditionNotMatch) if version moved
```

> **Real-S3 finding (2026-06, from the conformance harness).** S3's `If-None-Match` supports only the
> wildcard `*` (create-if-absent), so OpenDAL models that as the dedicated **`write_with_if_not_exists`**
> capability and reports **`write_with_if_none_match = false`** on S3 (it reserves if-none-match for
> arbitrary-ETag matching, which S3 does not support on writes). Use `.if_not_exists(true)` for ACQUIRE,
> not `.if_none_match("*")`. Likewise OpenDAL's `delete_with(...)` exposes no `if_match`, so RELEASE is a
> conditional **tombstone write** (`write_with(...).if_match(...)`), which `acquire` then CAS-takes.
>
> **GCS finding (2026-06) — Route A cannot run the lease on GCS.** OpenDAL's GCS backend reports
> **`write_with_if_match = false`**: GCS write preconditions are **generation-based** (`ifGenerationMatch`),
> not ETag/`If-Match`. OpenDAL surfaces the generation only as `if_not_exists` (=0) for *create* and has
> no arbitrary generation-match write option, so it cannot express the CAS **update** (renew/takeover) on
> GCS. This is a static capability gap, not an auth/access problem. **Therefore GCS uses Route B — the
> native GCS SDK** (`google-cloud-storage`, `ifGenerationMatch` for create=0 and swap=N; token =
> generation) behind the same `LeaseStore` trait. Route A (OpenDAL) covers **S3 + Azure** only.

A failed precondition surfaces as `Err(e)` with `e.kind() == ErrorKind::ConditionNotMatch` (a held
create may also surface `AlreadyExists`). **But** these calls are gated by a `CorrectnessCheckLayer`
against per-backend `Capability` flags — the ones the lease truly needs are **`write_with_if_not_exists`**
and **`write_with_if_match`** (no `delete_with_if_match`, since release tombstones via a conditional
write). If a backend build does not advertise a required flag, the call errors as *unsupported* rather
than executing. Therefore Route A requires:

1. **Pin a recent OpenDAL** — `if_match` on writes landed late-2024 / early-2025; the
   `iceberg-storage-opendal` transitive pin must be at or above that.
2. **Assert capabilities at startup** via `op.info().full_capability()` and **refuse to start** on a
   backend lacking the required flags (fail fast, not at first lease attempt).
3. **Verify the GCS mapping by test, not by docs** — confirm that the value OpenDAL hands back as the
   object "version" (what you feed into `if_match`) is the **generation**, and that OpenDAL
   translates it to `x-goog-if-generation-match` under the hood. This is the single riskiest
   assumption in the whole lease design.

### 17.3 Route B — native cloud SDKs

`aws-sdk-s3` (`put_object().if_none_match("*") / .if_match(etag)`), the GCS SDK
(`if_generation_match(0) / (generation)`), and `azure_storage_blob` (`If-None-Match`/`If-Match`).
Unambiguous per-provider semantics at the cost of three code paths and not reusing the operator the
sink already holds.

### 17.4 Recommendation and the conformance gate

Wrap the three ops behind a small trait with an **opaque version token**, default it over OpenDAL,
and make a **per-backend conformance suite the merge gate**:

```rust
trait LeaseStore {
    async fn acquire(&self, key: &str, body: &[u8]) -> Result<Version, LeaseError>;        // create-if-absent
    async fn swap(&self, key: &str, expected: &Version, body: &[u8]) -> Result<Version, LeaseError>; // CAS
    async fn release(&self, key: &str, expected: &Version) -> Result<(), LeaseError>;       // conditional delete
}
// LeaseError::Conflict  ⇐  ConditionNotMatch / 412 / 409 ConditionalRequestConflict
```

```
Conformance suite — run against MinIO / fake-gcs-server / Azurite, AND real services for T4/T5/T10:
  T1  acquire on empty key            → Ok(v0)
  T2  acquire on held key             → Conflict          # create-if-absent rejects
  T3  swap with correct version       → Ok(v1), v1 != v0  # CAS advances the token
  T4  swap with stale version         → Conflict          # ← the core safety property
  T5  N concurrent swaps, same v       → exactly 1 Ok, N-1 Conflict   # ← proves single-writer
  T6  release with correct version    → Ok; key gone/tombstoned
  T7  release with stale version      → Conflict
  T8  acquire immediately after release (no wait) → Ok    # planned-handoff fast path
  T9  S3: 409 ConditionalRequestConflict surfaces as Conflict (retryable), not a hard error
  T10 GCS: the token fed back into swap is the GENERATION, not the read ETag   # ← the GCS trap
```

T4 and T5 are what actually prove "at most one writer"; T10 guards the GCS trap; T9 guards the S3
contention code. Emulators are fine for functional coverage but at least **T4/T5/T10 must run against
the real services once** — emulators sometimes implement preconditions loosely.

**D-5 output:** approve **Route A (OpenDAL) as default**, with the startup capability assertion + the
conformance suite as a CI merge gate, and a documented **per-backend drop-to-native-SDK** escape hatch
if a capability or mapping gap is found. This is the `cluster.lock_backend: storage` implementation;
the optional `kubernetes` backend (D-2) is a separate `LeaseStore` impl over the K8s Lease API.

> **APPROVED + S3 & GCS PROVEN (2026-06).** The `LeaseStore` trait and the T1–T10 conformance suite live
> at **`prototypes/lease-conformance/`**. **S3 (Route A/OpenDAL): T1–T9 pass on real AWS S3, versioned
> and non-versioned. GCS (Route B/native `google-cloud-storage`): T1–T10 pass on real GCS, including T10
> — the generation-CAS gate.** Findings folded into §17.1/§17.2: create-if-absent is `if_not_exists` (S3
> reports `write_with_if_none_match = false`); the S3/Azure token is the **ETag** (a versioned bucket's
> `version()` is the version-id, which `If-Match` rejects); and **OpenDAL can't run the lease on GCS**
> (`write_with_if_match = false`), so GCS uses the native SDK with `ifGenerationMatch`. **Azure: OpenDAL
> 0.57 azblob ALSO reports `write_with_if_match = false`** (`if_not_exists=true, if_none_match=true`) — an
> OpenDAL limitation (Azure Blob natively supports `If-Match`). The project is pinned to OpenDAL 0.57, so
> **on 0.57 Route A covers S3 only**, and Azure is implemented as a native Route B (raw Blob REST +
> `reqwest` + SAS, `If-Match`/`If-None-Match`, ETag token). Net: `lock_backend: storage` is a per-backend
> `LeaseStore` — OpenDAL/S3, native/GCS, native/Azure — one trait, §5.3 protocol unchanged. D-5 stays
> **open-until the Azure run** before the storage lease is trusted in
> production. This is the only remaining clustering item that cannot be closed on paper.
>
> **Verification + fallback are consolidated** in `CDC_Design_Gap_Analysis_v1.md` DA-CLUS-1 / §24.1:
> the per-backend conflict-code table, the startup capability assertion (`op.info().full_capability()`
> → refuse to start if `write_with_if_not_exists` / `write_with_if_match` is absent — the real-S3
> finding: S3 reports `write_with_if_none_match = false`, so acquire uses `if_not_exists`), and the
> **per-backend drop-to-native-SDK** escape (Route B, §17.3) if OpenDAL lacks or mis-maps a capability —
> an isolated `LeaseStore` swap, not a change to the §5.3 protocol.

---

## 18. D-6 elaborated — Lease timing and the replication-lag budget

D-6 picks concrete `lease_duration` / `lease_renewal_interval` defaults by relating them to SWIM
detection, the worst-case failover window, and the end-to-end replication-lag SLA (OQ-OB2).

### 18.1 The failover timing chain (unplanned death)

```
t0  node A dies / partitions
      │  SWIM detection ≈ probe_interval + suspicion_mult · log10(N+1) · probe_interval
      │  with probe_interval=1s, suspicion_mult≈4, small N → ~4–6s
t1  peers mark A dead → ring recompute → B becomes owner of A's pipelines
      │  B runs takeover-evaluation: must observe the lease version UNCHANGED for a full
      │  lease_duration of LOCAL time before CAS-taking it  (B may start this clock at t1)
      │  ≈ lease_duration
t2  B CAS-acquires (one GET + one PUT ≈ 0.1–1s)
t3  B starts pipeline (Phase 0: read state, reconnect source, resume from checkpoint)
```

**Unowned window ≈ `SWIM_detect + lease_duration + acquire_latency`.** With `lease_duration = 15s`:
≈ 4–6s + 15s + ~1s ≈ **~20–22s** worst case. B must **not** shortcut the `lease_duration` wait — that
wait is the safety boundary against a still-alive A. (A *graceful* leave skips it entirely because the
lease is released; only unplanned death pays it — §8.3.)

### 18.2 Two invariants the timers must satisfy

1. **Renewal liveness (single-node property):** `lease_renewal_interval ≤ lease_duration / 3` — renew
   3× per TTL so the holder survives 2 consecutive lost/slow renewals before it must self-stop. With
   `15s / 5s` that is ~10s of slack.
2. **Pause tolerance (safety margin):** `lease_duration` must exceed the worst holder stall (GC,
   back-pressure stall, S3 latency spike) we are willing to treat as "still alive." A pause longer
   than `lease_duration` *correctly* lets a challenger take over; the brief overlap is closed by the
   §5.5 Iceberg fence. Set `lease_duration ≥ p99.9 tolerable stall`; for a heavy-load CDC worker,
   multi-second stalls are plausible, so 15s is a sane floor and Lifeguard reduces misclassification.

### 18.3 Coupling to the replication-lag SLA (OQ-OB2)

- **Steady state:** end-to-end lag is dominated by `flush_mode` (default 30s / 100k rows / 512 MB),
  **not** by lease timing. Renewal is one tiny conditional PUT per pipeline per interval, off the hot
  path — negligible.
- **During failover:** lag spikes by the **unowned window** (~20s) **plus** replay since the last
  checkpoint (bounded by the flush-driven checkpoint interval, per Data-Flow.md) **plus** Phase-0
  resume. So worst-case failover lag ≈ `unowned_window + checkpoint_interval + resume`. A 60s max-lag
  SLA fits the Balanced defaults with margin; a sub-10s recovery target forces a shorter
  `lease_duration` *and* tighter flush/checkpoint cadence.

### 18.4 The trade table (the actual decision)

| Profile | `lease_duration` | `lease_renewal_interval` | ~unowned window | Cost / risk |
|---|---|---|---|---|
| **Fast failover** | 5s | 1.5s | ~10–12s | More renewal PUTs; higher false-takeover risk under S3 tail latency; less pause tolerance |
| **Balanced (recommended)** | **15s** | **5s** | ~20–22s | Good pause tolerance; modest renewal traffic |
| **Conservative** | 30s | 10s | ~35–40s | Very stall-tolerant; slow failover; cheapest renewal traffic |

Renewal volume = `pipelines_owned / renewal_interval` PUT/s per worker — 100 pipelines at 5s ≈ 20
conditional PUT/s, trivially below any throttle. **Cost is not the constraint**; the choice is purely
failover-speed vs false-takeover/pause-tolerance.

### 18.5 Consistency and SWIM-timer interactions

- **`lease_duration` must be cluster-wide identical** (a challenger with a shorter value could take
  over while a slower-configured holder still thinks its lease is valid → overlap the local-time
  argument does not cover). Pin it in the `globals.state` cluster anchor next to `tokens` (§12); PROC.
  `lease_renewal_interval` is a local-liveness knob; keep it uniform for simplicity; PROC.
- Keep `lease_duration ≤ dead_node_reclaim_time` (15s ≤ 30s ✓) so a dead node's membership entry is
  not reclaimed before its lease is takeable.
- Keep `lease_duration ≫ probe_interval + suspicion` (15s ≫ ~5s ✓) so normal SWIM jitter never races
  the lease.

### 18.6 D-6 output — RATIFIED (Balanced, 2026-06-20)

**Signed off:** ship **Balanced** as the default profile (`lease_duration = 15s`,
`lease_renewal_interval = 5s`), ~20–22s worst-case unowned window, fitting a ~60s max-lag SLA with
margin. Fast (5s/1.5s) is the documented opt-in for latency-critical deployments with validated low S3
tail latency; Conservative (30s/10s) for high-stall workloads. The two OQ-OB2 feedbacks below remain
open as *tuning inputs*, not blockers — the defaults stand until load tests measure real p99 worker
stalls.

Ship **Balanced** defaults (`lease_duration = 15s`, `lease_renewal_interval = 5s`); both are config
(§11), so operators tune per environment, and the trade table is the guidance. Revisit once **OQ-OB2
fixes a numeric lag SLA** and load tests measure real p99 worker stalls under back-pressure. Treat
**Fast failover** as opt-in for latency-critical deployments that have validated low S3 tail latency.
Two sub-decisions feed back into OQ-OB2: (1) fix the numeric end-to-end lag SLA so the profile choice
is grounded, and (2) decide whether **failover-time** carries its own SLA distinct from steady-state
lag (it usually should — a ~20s recovery blip can be acceptable even under a tight steady-state target
if consumers tolerate occasional gaps, which the `_lsn` dedup already assumes).

---

## 19. D-8 elaborated — Lease renewal failure taxonomy and the safety deadline

### 19.1 The bug this closes

§5.3's renew rule says: *"If `412` → I have lost the lease … immediately stop the pipeline and do not
commit. Renewal failure is a hard stop, not a warning."* That single sentence **conflates two
fundamentally different failure classes**:

- **"The arbiter told me I lost"** — a `412` means a challenger CAS-advanced the lease object. This is
  a *definitive loss* and must be an immediate hard stop.
- **"I could not reach the arbiter"** — a timeout, `5xx`, throttle, or connection reset means the
  lease *status is unknown*, not lost. The holder very likely still owns the lease; it simply could
  not refresh its proof this round.

Treating the second as a hard stop is wrong in both directions: it needlessly drops a healthy
holder's pipelines on every transient storage blip, and — acutely — in single-node mode (§10) it
stops the *only* worker for a 2-second S3 hiccup, manufacturing an outage where none existed. The fix
is to classify the renew outcome and drive a small state machine off it, bounded by a **safety
deadline** that preserves the clock-skew-safe guarantee of §5.3.

### 19.2 Renew outcome taxonomy

Outcomes of a renew `PUT leases/<id>.json If-Match: <my_etag>`:

| Outcome | Meaning | Class | Action |
|---|---|---|---|
| `200/201` | ETag/generation advanced, still mine | success | `last_renew_local = now()`; store new version → stay **Held** |
| `412 Precondition Failed` | A challenger CAS-took the lease | **definitive loss** | → **Lost**: signal hard-stop before next commit; no retry |
| `409 ConditionalRequestConflict` (S3 only) | Concurrent conditional-write contention, *not* a loss | transient/contention | re-`GET`: if holder still me → retry `PUT`; if holder changed → **Lost**. Counts against the deadline |
| timeout / `5xx` / `503 SlowDown` / `429` / conn-reset / DNS | Arbiter unreachable; status unknown | **transient** | → **Degraded**: retry with jittered backoff until success (→ Held) or the deadline (→ Lost) |
| `404 Not Found` | Lease object absent (external delete) | ambiguous | → **Lost**, stop; the reconcile loop re-acquires via `If-None-Match:*` (D-8 decision: stop-first, not in-place re-acquire) |
| `403` / permanent auth | Credentials/permission fault | permanent | → **Lost**, stop **and alert** — operator issue, not lease contention |

The acquire and takeover-evaluation paths (§5.3) are unchanged; this taxonomy governs **renew only**.

### 19.3 The safety deadline (the load-bearing addition)

A holder may continue running and committing **only while it can still prove a valid, unexpired
lease.** The proof is a successful renew that advances the version. The holder must therefore hard-stop
by, measured on its **own** clock:

```
deadline = last_renew_local + lease_duration − guard
```

where `last_renew_local` is the local timestamp of the holder's last *successful* renew or acquire,
and `guard = lease_duration × lease_safety_margin` (default `lease_safety_margin = 0.15`; see §19.6).

**Why this is still clock-skew-safe.** Both the holder's `deadline` and the challenger's
takeover-evaluation wait (§5.3 step 2) are *durations* anchored to the **same physical event**: the
ETag write at `last_renew_local`. A challenger can only `GET` that version at a real time
≥ `last_renew_local`, and it waits a full `lease_duration` on *its* clock before CAS-taking. So if the
holder stops by its local `last_renew_local + lease_duration`, the holder's run-window and the
challenger's take cannot overlap — modulo clock-*rate* drift (not absolute offset). `guard` absorbs
that bounded rate drift over one TTL plus the time to actually halt in-flight commits. No node ever
compares its clock to another's; the protocol depends only on the existing bounded-drift-rate
assumption from §5.3 / §7.3.

This is the precise relationship the §18.2 invariant `lease_renewal_interval ≤ lease_duration / 3`
exists to serve: it guarantees the Degraded state has budget for **two** failed renew attempts before
the deadline. With Balanced defaults (15s / 5s, `guard ≈ 2.25s`): a renew succeeds at `T`, retries
fire at `T+5` and `T+10`, and the deadline lands at `≈ T+12.75` — room for two transient failures to
recover before the holder must stop.

### 19.4 The renewer state machine

```
              ┌────────────────────────── 200 (version advanced) ──────────────────────────┐
              │                                                                             │
              ▼                                                                             │
        ┌──────────┐   renew due        ┌───────────┐   200            ┌──────────┐         │
        │   HELD   │ ─────────────────► │ RENEWING  │ ───────────────► │   HELD   │ ────────┘
        └────┬─────┘                    └─────┬─────┘                  └──────────┘
             │                                │
             │                                ├─ 412 / 404 / 403 ───────────────► ┌────────┐
             │                                │                                   │  LOST  │──► hard-stop
             │                                ├─ transient / 409 ──► ┌──────────┐ └────────┘    (halt worker
             │                                │                      │ DEGRADED │                 before next
             │                                │   backoff+jitter ◄───┤ retrying │                 commit)
             │                                │   retry            ─►└────┬─────┘
             │                                │                           │ 200 → HELD (reset last_renew_local)
             │                                │                           │ now() ≥ deadline → LOST
             │                                │
   leave/drain (§8.3) ─► RELEASED (CAS delete/tombstone)
```

- **HELD → RENEWING**: every `lease_renewal_interval`.
- **RENEWING → HELD**: `200`; reset `last_renew_local`, store the new version token, clear any Degraded
  backoff.
- **RENEWING → LOST**: `412` (taken), `404` (deleted), `403`/permanent (operator fault). Lost is
  terminal for this ownership; the per-pipeline renewer signals the `PipelineWorker` to halt (via the
  `tokio::sync::watch` / cancellation token already in §13) *before* its next Iceberg commit, then the
  reconcile loop (§8.1) re-evaluates ownership.
- **RENEWING → DEGRADED**: transient error or S3 `409`. Retry with jittered exponential backoff capped
  well below `lease_renewal_interval` (so multiple attempts fit each interval).
- **DEGRADED → HELD**: any retry succeeds.
- **DEGRADED → LOST**: `now() ≥ deadline` reached without a successful renew. This is the *only* path
  where "couldn't reach storage" becomes a stop — and only after the full safety budget is spent.

### 19.5 Commit gating (D-8 decision #3)

The owner's pipeline gates each Iceberg commit on a cheap local check at **commit-start**:

```
may_commit := renewer_state ∈ {HELD, DEGRADED} ∧ now() < deadline
```

We deliberately do **not** estimate commit duration to decide whether a commit "will finish in time."
A commit that begins just under the deadline and slips past it is caught by the **Iceberg atomic
commit fence** (§5.5): the new owner has already advanced the table metadata, so the zombie's commit
conflicts and fails. Gate at start, let the fence backstop the slip. This keeps the gate to a single
comparison on the hot path and avoids a fragile duration model. The pre-commit lease re-check of §5.5
remains, but it is now understood as *"is the renewer still within its deadline?"* — answerable from
local state without a synchronous `GET` on every commit (the renewer's background cadence already
maintains the proof).

### 19.6 Config and validation additions

Extends the §11 table:

| Field | Type | Default | Mutability | Cluster-wide consistent? | Notes |
|---|---|---|---|---|---|
| `cluster.lease_safety_margin` | float (0–0.5) | `0.15` | PROC | Recommended equal | Fraction of `lease_duration` reserved as `guard` before the renewal safety deadline (§19.3). Scales with the chosen timing profile rather than being a fixed constant. |

And promotes the previously-advisory ratio to an **enforced** rule (D-8 decision #4), since the
Degraded retry budget now depends on it:

> **New validation rule (proposed for config spec §13):** `lease_renewal_interval ≤ lease_duration / 3`
> is a **hard error** (was a "should" in §11 / §18.2). Rationale: the renewal-failure safety deadline
> (§19.3) relies on the holder getting at least two renew attempts before it must self-stop. A
> renewal interval above `lease_duration / 3` silently removes that budget and turns a single slow
> renew into an unnecessary failover. Recommend `lease_safety_margin ∈ [0.05, 0.30]` with a warning
> outside that band.

(The validation rule lands in `Configuration_Specification.md`, which Oleksii owns — flagged as a
config-spec follow-up in the gap report, not edited here.)

### 19.7 Single-node and observability

- **Single-node (§10):** the taxonomy makes the §7.2 storage-correlation argument *explicit*. A
  storage blip shorter than the deadline no longer stops the sole worker (Degraded → recovers → Held);
  only a sustained outage longer than `lease_duration` stops it — and during such an outage the
  pipeline cannot commit anyway (storage is the sink), so the stop is harmless, not a regression.
- **Observability (D-8 decision #5, folds into G-33):** emit
  - `cdc_lease_state{pipeline}` gauge — `held | degraded | lost` (enum)
  - `cdc_lease_renew_failures_total{pipeline, class}` counter — `transient | conflict | lost`
  - `cdc_lease_renew_latency_seconds{pipeline}` histogram

  Degraded is the early-warning signal: operators see storage flapping *before* it escalates to a
  failover. A rising `transient` count with no `lost` means "storage is wobbling but safety held."

### 19.8 D-8 output

Adopt the renew-outcome taxonomy (§19.2), the safety deadline `deadline = last_renew_local +
lease_duration − guard` with `guard = lease_duration × lease_safety_margin` (default `0.15`, §19.3),
and the renewer state machine (§19.4). Gate commits at start on `state ∈ {HELD, DEGRADED} ∧ now() <
deadline`, with the Iceberg CAS fence as the slip backstop (§19.5). Add `cluster.lease_safety_margin`
(§19.6) and promote `lease_renewal_interval ≤ lease_duration / 3` to an enforced config-validation
rule. Tighten the §5.3 renew rule to reference this section. The §13 implementation sketch's
per-pipeline renewer owns this state machine and the `watch`/cancellation signal into the
`PipelineWorker`.

---

## 20. D-9 / D-10 / D-11 elaborated — reconciler robustness, takeover rate-limiting, flap immunity

These three close the remaining clustering correctness sub-gaps surfaced while reading §8/§13 as the
implementation contract. They are independent but share a refactor: the single reconcile loop of §8.1
becomes a **cluster reconciler + a per-pipeline controller**, which is the natural home for the lease
renewer (§19), the takeover cap (D-10), and the flap rules (D-11).

### 20.1 D-9 — per-pipeline controller model (reconciler ↔ renewer orchestration)

**The gap.** §8.1 sketches one periodic loop that, per pipeline, calls `ensure_lease_held` — which can
**block a full `lease_duration`** inside takeover-evaluation (§5.3) — and `ensure_running`. A single
loop doing that inline stalls every *other* pipeline behind the one being taken over. Separately, §13
lists per-pipeline renewer tasks, but the boundary between "the loop" and "the renewer" — and who is
allowed to write the lease — is never drawn.

**The model.** Split responsibilities into two layers:

- **`ClusterReconciler` (one per node)** owns *only the ring*. On a membership change, a config-
  generation advance, or a periodic resync tick, it recomputes `ring_owner(P)` for every pipeline and
  pushes each pipeline's **target** (`Own` / `DontOwn`) to that pipeline's controller. It **never reads
  or writes a lease**. It is cheap, non-blocking, and purely a function of membership + config.
- **`PipelineController` (one task/actor per pipeline)** owns the full per-pipeline lifecycle and is the
  **sole writer of that pipeline's lease object.** It runs this state machine:

  ```
  Unowned ──target=Own──► Acquiring ──CAS ok──► Owned(running) ──target=DontOwn──► Releasing ──► Unowned
     ▲                       │  (acquire §5.3 / takeover-evaluate, may wait lease_duration)
     │                       │
     └───────────────────────┴── target=DontOwn before acquire ─► Unowned (abandon)
                                                       Owned ──renew 412/404/403 (§19)──► Lost ─► Unowned
  ```

  The lease **renewer** (the §19 `Held/Renewing/Degraded/Lost` machine) lives *inside* the controller's
  `Owned` state. The (possibly long) takeover-evaluation wait happens inside the controller's own task,
  so it never blocks peers.

**Why this resolves the orchestration race.** The lease has exactly **one writer per pipeline at a
time — the controller in `Acquiring`/`Owned`.** The `ClusterReconciler` only ever *advises* (pushes a
target); it cannot race the renewer because it never PUTs a lease. `ensure_lease_held` / `ensure_running`
from §8.1 are now *internal controller transitions*, not loop-body calls.

**Trigger model — level-triggered + event-driven.** Following the Kubernetes-controller pattern: the
reconciler acts on **events** (membership change, config generation advance) for low-latency reaction,
**and** runs a **periodic resync** (cadence ≈ `lease_renewal_interval`) that re-pushes targets
unconditionally as a safety backstop against missed events. Controllers are **idempotent / level-
triggered**: re-receiving `Own` while already `Owned(running)` is a no-op. This is what makes the
at-most-one / at-least-one reasoning (§5.4) hold under dropped or reordered events — convergence does
not depend on seeing every edge.

§8.1's pseudocode remains a correct *description of the desired state*; §20.1 specifies *how it is
executed* without head-of-line blocking. §13's `Reconciler` + `LeaseManager` boxes are refined into
`ClusterReconciler` (ring only) + N × `PipelineController` (lease + worker + renewer).

### 20.2 D-10 — unplanned-takeover storm rate-limiting

**The gap.** `cluster.max_concurrent_handoffs` (§8.5, default 2) caps only **planned drains**. On an
**unplanned death** of a node holding many pipelines, every survivor that becomes ring-owner starts
takeover + Phase-0 (source reconnect, state read, resume) at once — a reconnect/state-read burst
against the source DBs and object storage exactly when the system is already perturbed. In a small
cluster the ring concentrates the dead node's pipelines onto few survivors, amplifying the burst.

**The mechanism.** Add a per-node cap, `cluster.max_concurrent_takeovers` (default `4`), that gates
**concurrent Phase-0 starts** on the unplanned path — i.e. how many pipelines a node will simultaneously
*bring into running* (reconnect + resume). It deliberately does **not** gate the takeover-evaluation
*wait*: that wait is a local timer plus two `GET`s (§5.3), is cheap, and may run for all pending
pipelines in parallel so that none of their safety clocks are delayed. Only the expensive, load-bearing
Phase-0 start is queued behind the cap. Add small randomized **jitter** to each Phase-0 start to
de-synchronize reconnect bursts even within the cap.

**Scope and trade-off.** The cap is **per-node-global** for now; the sharper risk — many pipelines
sharing one source endpoint reconnecting together — is noted as a future **per-source-endpoint**
concurrency group (analogous to a bulkhead). The cap trades failover *latency* for source-DB
*protection*: the `(owned − cap)` pipelines recover in waves, so a node taking over many pipelines at
once has a longer tail. Default `4` (> `max_concurrent_handoffs = 2`) reflects that takeover is the
urgent, unplanned path and should drain its queue faster than a planned rebalance, while still capping
the burst. This is a tuning knob, not a safety boundary — the lease still guarantees at-most-one
regardless of the cap.

### 20.3 D-11 — flap immunity (suspect-stays-in-ring + symmetric stabilization)

**The gap.** `cluster.rebalance_stabilization` (§8.5) was specified for *first-time joins* only. Two
membership perturbations were unhandled: a node going `suspect` (a transient blip / brief GC), and a
previously-`dead` node **rejoining**. Both change the derived ring and can cause drain→reverse churn.

**The rules.**

| Membership transition | Ring effect | Timing |
|---|---|---|
| node `alive → suspect` | **none — suspect stays in the ring** | n/a (no recompute) |
| node `suspect → alive` (recovered) | **none** (it never left the ring) | n/a |
| node `→ dead` / `→ left` | **removed** from ownership computation | **immediate** (urgent; lease-expiry-driven for `dead`, lease-release-driven for `left`) |
| node joins (new) **or rejoins** (dead→Active) | **added** to ownership computation | **delayed** by `rebalance_stabilization` + `Joining→Active` hysteresis |

- **Suspect nodes remain in the ring** (ring membership = `(alive ∨ suspect) ∧ phase ∈ {Active,
  Draining}`). This is safe because **safety is the lease, not the ring** (§8.4): a node that is truly
  dead transitions to `dead` shortly and the lease-expiry takeover (§8.2) handles it; a node that
  recovers caused **zero** ring churn. A suspect node also keeps renewing its own lease, so any peer's
  speculative takeover-evaluation sees the ETag advancing and backs off. This is the direct payoff of
  the §3.6 / §8.4 "SWIM accuracy is a latency concern, not a safety concern" stance.
- **Symmetric stabilization:** ring *removals* act **immediately** (the dead-node failover path must
  not be delayed — it is driven by lease expiry, its own timer); ring *additions* — **both** new joins
  **and rejoins** of a returning node — wait out `rebalance_stabilization` before proactive rebalancing
  moves any pipeline, and the added node passes `Joining → Active` before it can own anything. A
  flapping node therefore never repeatedly grabs-and-drops pipelines, and a transient suspicion never
  triggers a handoff at all.

This removes the asymmetry hole in §8.5 without new config: it is a precise statement of *which*
membership transitions perturb the ring and how fast the system reacts to each.

### 20.4 Configuration additions

Extends the §11 table:

| Field | Type | Default | Mutability | Cluster-wide consistent? | Notes |
|---|---|---|---|---|---|
| `cluster.max_concurrent_takeovers` | integer | `4` | R | No | Cap on simultaneous **Phase-0 starts** on the unplanned-takeover path, per node (§20.2). Gates the expensive resume, not the cheap takeover-evaluation wait. Tuning knob, not a safety boundary. |

> **Config-spec follow-up (proposed, not applied — Oleksii owns the spec):** add
> `cluster.max_concurrent_takeovers` to `Configuration_Specification.md` §6.5 / §16 with validation
> `≥ 1` (and the existing `max_concurrent_handoffs` ≥ 1 pattern). No cluster-wide-consistency
> requirement (it is a local resource knob).

### 20.5 Output

Adopt the **`ClusterReconciler` + per-pipeline `PipelineController`** split (D-9): the reconciler owns
the ring and pushes targets; each controller owns its pipeline's lifecycle and is the sole lease-writer,
with the §19 renewer living in its `Owned` state; triggering is event-driven with a periodic resync,
and controllers are idempotent/level-triggered. Add `cluster.max_concurrent_takeovers` (default 4) to
cap concurrent Phase-0 starts on the unplanned path, with jitter, per-node-global scope, and a future
per-source-endpoint group noted (D-10). Keep **suspect nodes in the ring** (only `dead`/`left` removes
a node) and apply `rebalance_stabilization` to **all ring additions including rejoins**, while ring
removals act immediately (D-11). Refine §8.1 and §13 to reference this section.
