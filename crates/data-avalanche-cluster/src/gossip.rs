//! The membership plane (Clustering §2–§3, Q-M8-1): SWIM with infection-style dissemination and
//! suspicion, from foca 2.0, driven by one tokio task per node. The task owns the `Foca` state
//! and feeds it the packets the wire receives, the timers it asked for and the node's commands;
//! what foca sends goes out sealed (`GossipKey`), what arrives unsealed is dropped.
//!
//! foca knows a member by its address (probe finding 4), so a node is an *identity*: its name,
//! the incarnation of its process, a revision bumped on each metadata change and on each rejoin,
//! its address and its metadata. [`Members`] groups the identities by name: a node's current one
//! is its latest live identity, and a node is a member while that identity is alive or suspect.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::num::{NonZeroU8, NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use data_avalanche_telemetry::{Metrics, Suspicion};
use foca::{
    AccumulatingRuntime, Foca, Incarnation, Message, NoCustomBroadcast, OwnedNotification,
    PeriodicParams, PostcardCodec, ProbeNumber, State,
};
use rand::rngs::StdRng;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

use crate::wire::{GossipKey, MAX_PAYLOAD, Wire};

/// Where a node is in its life (Clustering §3.4). Only an `Active` node is placed on the ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    /// Started; not yet given pipelines.
    Joining,
    Active,
    /// Shutting down: handing its pipelines off.
    Draining,
    /// Its pipelines are released; about to leave the gossip.
    Leaving,
}

/// What a node tells the others about itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeMeta {
    pub phase: Phase,
    /// The config generation the node runs (`globals.state`, §12).
    pub config_generation: u64,
    /// The build, e.g. `1.0.0`.
    pub build: String,
    /// Where the others reach its API (`https://cdc-0.cdc.ns.svc:8080`, D-M13b.2).
    pub api_url: String,
    /// The first 16 hex digits of its own config file's hash; empty when it can't read it.
    pub file_hash: String,
}

/// What a node's gossip saw of its probes, for its page (D-M13b.8).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GossipFigures {
    /// The member the last probe pinged, and when.
    pub last_probe: Option<(String, DateTime<Utc>)>,
    /// That probe's round trip, once its ack came.
    pub probe_rtt: Option<Duration>,
    /// The members this node suspected (its probe of them failed), and when, in the last
    /// [`SUSPICIONS_KEPT`].
    pub raised: Vec<(String, DateTime<Utc>)>,
    /// When the others suspected this node, in the last [`SUSPICIONS_KEPT`]: it refuted (foca
    /// raises its own incarnation, which its packets carry), or it was declared down and
    /// rejoined.
    pub received: Vec<DateTime<Utc>>,
}

/// How long the figures keep a suspicion.
pub const SUSPICIONS_KEPT: Duration = Duration::from_secs(24 * 3600);

impl GossipFigures {
    /// Forgets the suspicions older than [`SUSPICIONS_KEPT`] before `now`.
    fn forget_before(&mut self, now: DateTime<Utc>) {
        let kept = chrono::Duration::from_std(SUSPICIONS_KEPT).unwrap_or(chrono::Duration::MAX);
        let since = now
            .checked_sub_signed(kept)
            .unwrap_or(DateTime::<Utc>::MIN_UTC);
        self.raised.retain(|(_, at)| *at >= since);
        self.received.retain(|at| *at >= since);
    }
}

/// One incarnation of a node, as foca knows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub node_id: String,
    /// The process's start, in Unix milliseconds: a restarted node's identity is newer.
    pub incarnation: u64,
    /// Bumped on every metadata change and every rejoin.
    pub rev: u32,
    /// The advertised address: the others send here.
    pub addr: SocketAddr,
    pub meta: NodeMeta,
}

impl NodeIdentity {
    fn version(&self) -> (u64, u32) {
        (self.incarnation, self.rev)
    }
}

impl foca::Identity for NodeIdentity {
    type Addr = SocketAddr;

    /// A node declared down while it runs (a partition, a pause) rejoins at once.
    fn renew(&self) -> Option<Self> {
        Some(Self {
            rev: self.rev.wrapping_add(1),
            ..self.clone()
        })
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn win_addr_conflict(&self, other: &Self) -> bool {
        self.version() > other.version()
    }
}

/// A node as the others see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub addr: SocketAddr,
    /// The member's process (its start in unix milliseconds): a lease's holder names it too.
    pub incarnation: u64,
    pub meta: NodeMeta,
    /// Suspected of having failed; still a member (D-11: suspects stay on the ring).
    pub suspect: bool,
}

/// The nodes one node counts as members, itself included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Members {
    pub me: String,
    pub nodes: BTreeMap<String, Member>,
}

impl Members {
    /// The other members.
    pub fn peers(&self) -> usize {
        self.nodes.len().saturating_sub(1)
    }

    /// The newest config generation a member applies, this node included (Clustering §9.1).
    pub fn newest_generation(&self) -> u64 {
        self.nodes
            .values()
            .map(|m| m.meta.config_generation)
            .max()
            .unwrap_or(0)
    }
}

/// The members `me` sees in foca's membership state: each name's latest identity that isn't
/// down (a restarted node's previous identity lingers as suspect until it is declared down), and
/// no name whose identities are all down.
fn members_of<'a>(
    me: &NodeIdentity,
    state: impl Iterator<Item = (&'a NodeIdentity, State)>,
) -> Members {
    let mut latest: BTreeMap<&str, (&NodeIdentity, State)> = BTreeMap::new();
    for (id, s) in state {
        if id.node_id == me.node_id || s == State::Down {
            continue;
        }
        let e = latest.entry(&id.node_id).or_insert((id, s));
        if id.version() > e.0.version() {
            *e = (id, s);
        }
    }
    let mut nodes: BTreeMap<String, Member> = latest
        .into_iter()
        .map(|(name, (id, s))| {
            let member = Member {
                addr: id.addr,
                incarnation: id.incarnation,
                meta: id.meta.clone(),
                suspect: s == State::Suspect,
            };
            (name.to_owned(), member)
        })
        .collect();
    let mine = Member {
        addr: me.addr,
        incarnation: me.incarnation,
        meta: me.meta.clone(),
        suspect: false,
    };
    nodes.insert(me.node_id.clone(), mine);
    Members {
        me: me.node_id.clone(),
        nodes,
    }
}

/// The gossip's timers (config §6.5.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipSettings {
    pub probe_interval: Duration,
    pub probe_timeout: Duration,
    pub gossip_interval: Duration,
    pub gossip_nodes: u32,
    pub push_pull_interval: Duration,
    pub retransmit_factor: u32,
}

impl GossipSettings {
    pub fn new(m: &data_avalanche_config::Memberlist) -> Self {
        Self {
            probe_interval: m.probe_interval,
            probe_timeout: m.probe_timeout,
            gossip_interval: m.gossip_interval,
            gossip_nodes: m.gossip_nodes,
            push_pull_interval: m.push_pull_interval,
            retransmit_factor: m.retransmit_factor,
        }
    }
}

/// How long a suspect has to refute before it is declared down, as a multiple of
/// `max(1, log10 n) · probe_interval` (memberlist's `SuspicionMult`).
const SUSPICION_MULT: f64 = 4.0;

/// How many members a probe asks to probe indirectly.
const INDIRECT_PROBES: usize = 3;

/// How long a down member is remembered, and announced to so that a healed partition rejoins
/// (foca's default). A newer identity of the same node is accepted at once whatever this is.
const REMEMBER_DOWN: Duration = Duration::from_secs(24 * 60 * 60);

/// foca's settings for a cluster of `n` members. The settings that scale with the cluster are
/// set again whenever its size changes; the others never change (foca refuses that).
fn foca_config(s: &GossipSettings, n: usize) -> foca::Config {
    let n = u32::try_from(n.max(1)).unwrap_or(u32::MAX);
    let log = f64::from(n).log10();
    let tx = s.retransmit_factor as f64 * f64::from(n.saturating_add(1)).log10().ceil();
    let every = |frequency, num| PeriodicParams {
        frequency,
        num_members: NonZeroUsize::new(num).unwrap_or(NonZeroUsize::MIN),
    };
    let mut c = foca::Config::new_lan(NonZeroU32::MIN);
    c.probe_period = s.probe_interval;
    c.probe_rtt = s.probe_timeout;
    c.num_indirect_probes = NonZeroUsize::new(INDIRECT_PROBES).unwrap_or(NonZeroUsize::MIN);
    c.max_transmissions = NonZeroU8::new(tx.clamp(1.0, 255.0) as u8).unwrap_or(NonZeroU8::MIN);
    c.suspect_to_down_after = s.probe_interval.mul_f64(SUSPICION_MULT * log.max(1.0));
    c.remove_down_after = Some(REMEMBER_DOWN);
    c.max_packet_size = NonZeroUsize::new(MAX_PAYLOAD).unwrap_or(NonZeroUsize::MIN);
    c.notify_down_members = true;
    c.periodic_gossip = Some(every(s.gossip_interval, s.gossip_nodes as usize));
    c.periodic_announce = Some(every(s.push_pull_interval, 1));
    c.periodic_announce_to_down_members = Some(every(s.push_pull_interval, 2));
    c
}

/// How often at most a dropped packet is warned about.
const WARN_EVERY: Duration = Duration::from_secs(60);

enum Command {
    Announce(Vec<SocketAddr>),
    Phase(Phase),
    Generation(u64),
    FileHash(String),
    Metrics(Metrics),
    Leave(oneshot::Sender<()>),
}

/// Sends a running gossip's announces; cheap to clone.
#[derive(Debug, Clone)]
pub struct Announcer(mpsc::UnboundedSender<Command>);

impl Announcer {
    /// Asks `seeds` to let this node in. A seed is known by its address only, and that must be
    /// the one the seed advertises: foca drops an announce addressed elsewhere.
    pub fn announce(&self, seeds: Vec<SocketAddr>) {
        let _ = self.0.send(Command::Announce(seeds));
    }
}

/// Tells the others the config generation this node applies; cheap to clone.
#[derive(Debug, Clone)]
pub struct Advertiser(mpsc::UnboundedSender<Command>);

impl Advertiser {
    pub fn set_config_generation(&self, generation: u64) {
        let _ = self.0.send(Command::Generation(generation));
    }

    /// Tells the others the hash of this node's config file; false once the gossip is gone.
    pub fn set_file_hash(&self, hash: String) -> bool {
        self.0.send(Command::FileHash(hash)).is_ok()
    }
}

/// A running gossip. Dropping it stops the gossip without a leave, as a crash would.
#[derive(Debug)]
pub struct Gossip {
    commands: mpsc::UnboundedSender<Command>,
    members: watch::Receiver<Members>,
    figures: watch::Receiver<GossipFigures>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Gossip {
    fn drop(&mut self) {
        self.task.abort();
    }
}

type Swim = Foca<NodeIdentity, PostcardCodec, StdRng, NoCustomBroadcast>;

impl Gossip {
    /// Starts the gossip as `me`: it sends through `wire` and reads what `inbox` receives.
    pub fn start(
        me: NodeIdentity,
        key: GossipKey,
        settings: GossipSettings,
        wire: Arc<dyn Wire>,
        inbox: mpsc::Receiver<Bytes>,
    ) -> Self {
        let swim = Foca::new(
            me.clone(),
            foca_config(&settings, 1),
            rand::make_rng(),
            PostcardCodec,
        );
        let (members_tx, members) = watch::channel(members_of(&me, std::iter::empty()));
        let (figures_tx, figures) = watch::channel(GossipFigures::default());
        let (commands, rx) = mpsc::unbounded_channel();
        let actor = Actor {
            swim,
            key,
            settings,
            wire,
            members: members_tx,
            figures: figures_tx,
            metrics: None,
            probe: None,
            incarnation: None,
            size: 1,
            dropped: 0,
            warned: None,
        };
        let task = tokio::spawn(actor.run(inbox, rx));
        Self {
            commands,
            members,
            figures,
            task,
        }
    }

    /// What the gossip saw of its probes, as it changes.
    pub fn figures(&self) -> watch::Receiver<GossipFigures> {
        self.figures.clone()
    }

    /// Counts the probes' round trips and the suspicions in `metrics` from now on.
    pub fn report_to(&self, metrics: Metrics) {
        let _ = self.commands.send(Command::Metrics(metrics));
    }

    /// The members, as they change.
    pub fn members(&self) -> watch::Receiver<Members> {
        self.members.clone()
    }

    /// Asks `seeds` to let this node in. A seed is known by its address only, and that must be
    /// the one the seed advertises: foca drops an announce addressed elsewhere.
    pub fn announce(&self, seeds: Vec<SocketAddr>) {
        self.announcer().announce(seeds);
    }

    /// A handle that announces for this gossip, for a task that outlives a borrow of it
    /// (discovery).
    pub fn announcer(&self) -> Announcer {
        Announcer(self.commands.clone())
    }

    /// Tells the others this node's new phase.
    pub fn set_phase(&self, phase: Phase) {
        let _ = self.commands.send(Command::Phase(phase));
    }

    /// A handle that tells the others this node's config generation, for a task that outlives
    /// a borrow of the gossip (the config coordinator).
    pub fn advertiser(&self) -> Advertiser {
        Advertiser(self.commands.clone())
    }

    /// Stops the gossip as a crash would, keeping the handle: what it sends goes nowhere.
    #[cfg(test)]
    pub(crate) fn abort(&self) {
        self.task.abort();
    }

    /// Leaves the cluster: the others see this node gone at once, not after a suspicion.
    pub async fn leave(self) {
        let (tx, rx) = oneshot::channel();
        if self.commands.send(Command::Leave(tx)).is_ok() {
            let _ = rx.await;
        }
    }
}

struct Actor {
    swim: Swim,
    key: GossipKey,
    settings: GossipSettings,
    wire: Arc<dyn Wire>,
    members: watch::Sender<Members>,
    figures: watch::Sender<GossipFigures>,
    metrics: Option<Metrics>,
    /// The last probe's ping not yet acked: its number and when it went.
    probe: Option<(ProbeNumber, Instant)>,
    /// This node's incarnation as its last packet carried it.
    incarnation: Option<Incarnation>,
    /// The cluster size foca's settings were last computed for.
    size: usize,
    dropped: u64,
    warned: Option<Instant>,
}

impl Actor {
    async fn run(
        mut self,
        mut inbox: mpsc::Receiver<Bytes>,
        mut commands: mpsc::UnboundedReceiver<Command>,
    ) {
        let mut rt = AccumulatingRuntime::new();
        let (timers_tx, mut timers) = mpsc::unbounded_channel();
        loop {
            let done = tokio::select! {
                Some(packet) = inbox.recv() => {
                    match self.key.open(&packet) {
                        Some(payload) => log(self.swim.handle_data(payload, &mut rt)),
                        None => self.dropped(),
                    }
                    None
                }
                Some(timer) = timers.recv() => {
                    log(self.swim.handle_timer(timer, &mut rt));
                    None
                }
                command = commands.recv() => match command {
                    Some(Command::Announce(seeds)) => {
                        for addr in seeds {
                            log(self.swim.announce(seed(addr), &mut rt));
                        }
                        None
                    }
                    Some(Command::Phase(phase)) => {
                        let me = self.swim.identity();
                        let next = NodeIdentity {
                            rev: me.rev.wrapping_add(1),
                            meta: NodeMeta { phase, ..me.meta.clone() },
                            ..me.clone()
                        };
                        log(self.swim.change_identity(next, &mut rt));
                        None
                    }
                    Some(Command::Generation(config_generation)) => {
                        let me = self.swim.identity();
                        let next = NodeIdentity {
                            rev: me.rev.wrapping_add(1),
                            meta: NodeMeta {
                                config_generation,
                                ..me.meta.clone()
                            },
                            ..me.clone()
                        };
                        log(self.swim.change_identity(next, &mut rt));
                        None
                    }
                    Some(Command::FileHash(file_hash)) => {
                        let me = self.swim.identity();
                        let next = NodeIdentity {
                            rev: me.rev.wrapping_add(1),
                            meta: NodeMeta {
                                file_hash,
                                ..me.meta.clone()
                            },
                            ..me.clone()
                        };
                        log(self.swim.change_identity(next, &mut rt));
                        None
                    }
                    Some(Command::Metrics(metrics)) => {
                        self.metrics = Some(metrics);
                        None
                    }
                    Some(Command::Leave(done)) => {
                        log(self.swim.leave_cluster(&mut rt));
                        Some(Some(done))
                    }
                    // The handle is gone.
                    None => Some(None),
                },
            };
            while let Some((to, payload)) = rt.to_send() {
                self.wire.send(to.addr, self.key.seal(&payload));
            }
            while let Some((after, timer)) = rt.to_schedule() {
                let tx = timers_tx.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(after).await;
                    let _ = tx.send(timer);
                });
            }
            while let Some(n) = rt.to_notify() {
                self.notified(n);
            }
            if let Some(done) = done {
                if let Some(done) = done {
                    let _ = done.send(());
                }
                return;
            }
            self.publish();
        }
    }

    fn publish(&mut self) {
        let me = self.swim.identity();
        let next = members_of(
            me,
            self.swim
                .iter_membership_state()
                .map(|m| (m.id(), m.state())),
        );
        let size = next.nodes.len();
        if size != self.size {
            self.size = size;
            log(self.swim.set_config(foca_config(&self.settings, size)));
        }
        self.members.send_if_modified(|m| {
            let changed = *m != next;
            *m = next;
            changed
        });
    }

    /// Keeps the figures (D-M13b.8): a ping this node sends starts a probe, its ack ends it; a
    /// failed probe is a suspicion raised; a packet carrying a higher incarnation of this node
    /// is a suspicion of it refuted, and a rejoin one that ended in its being declared down.
    fn notified(&mut self, n: OwnedNotification<NodeIdentity>) {
        let now = Utc::now();
        match n {
            OwnedNotification::DataSent(h) => {
                if let Message::Ping(probe) = h.message {
                    self.probe = Some((probe, Instant::now()));
                    self.figures.send_modify(|f| {
                        f.last_probe = Some((h.dst.node_id.clone(), now));
                        f.probe_rtt = None;
                    });
                }
                if self.incarnation.is_some_and(|i| h.src_incarnation > i) {
                    self.suspected(now);
                }
                self.incarnation = Some(h.src_incarnation);
            }
            OwnedNotification::DataReceived(h) => {
                if let (Message::Ack(probe), Some((sent, at))) = (h.message, self.probe)
                    && probe == sent
                {
                    self.probe = None;
                    let rtt = at.elapsed();
                    self.figures.send_modify(|f| f.probe_rtt = Some(rtt));
                    if let Some(m) = &self.metrics {
                        m.gossip_probe_rtt(rtt);
                    }
                }
            }
            OwnedNotification::ProbeFailed(_, member) => {
                self.figures.send_modify(|f| {
                    f.raised.push((member.node_id, now));
                    f.forget_before(now);
                });
                if let Some(m) = &self.metrics {
                    m.gossip_suspicion(Suspicion::Raised);
                }
            }
            OwnedNotification::Rejoin(me) => {
                tracing::info!("gossip: declared down by the others; rejoined as {me:?}");
                self.incarnation = None;
                self.suspected(now);
            }
            other => tracing::debug!("gossip: {other:?}"),
        }
    }

    /// The others suspected this node.
    fn suspected(&mut self, now: DateTime<Utc>) {
        self.figures.send_modify(|f| {
            f.received.push(now);
            f.forget_before(now);
        });
        if let Some(m) = &self.metrics {
            m.gossip_suspicion(Suspicion::Received);
        }
    }

    /// A packet that doesn't open: another cluster's, a corrupted one, or forged.
    fn dropped(&mut self) {
        self.dropped += 1;
        if self.warned.is_none_or(|t| t.elapsed() >= WARN_EVERY) {
            tracing::warn!(
                dropped = self.dropped,
                "gossip: dropped packets whose MAC doesn't match this cluster's key: another \
                 cluster's nodes on this network, or a node of this cluster on another anchor"
            );
            self.warned = Some(Instant::now());
        }
    }
}

/// An announce's destination: foca needs an identity, but only its address is used.
fn seed(addr: SocketAddr) -> NodeIdentity {
    NodeIdentity {
        node_id: String::new(),
        incarnation: 0,
        rev: 0,
        addr,
        meta: NodeMeta {
            phase: Phase::Joining,
            config_generation: 0,
            build: String::new(),
            api_url: String::new(),
            file_hash: String::new(),
        },
    }
}

/// foca's errors are about a packet or a call it refused; the protocol recovers from them.
fn log(r: Result<(), foca::Error>) {
    if let Err(e) = r {
        tracing::debug!("gossip: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{Hub, at, eventually, gossip_settings as settings};

    fn identity(name: &str, incarnation: u64, addr: SocketAddr) -> NodeIdentity {
        NodeIdentity {
            node_id: name.into(),
            incarnation,
            rev: 0,
            addr,
            meta: NodeMeta {
                phase: Phase::Active,
                config_generation: 1,
                build: "1.1.0".into(),
                api_url: format!("http://{}", SocketAddr::new(addr.ip(), 8080)),
                file_hash: "0123456789abcdef".into(),
            },
        }
    }

    const KEY: [u8; 32] = [7; 32];

    fn start(hub: &Arc<Hub>, name: &str, incarnation: u64, addr: SocketAddr) -> Gossip {
        start_with(hub, name, incarnation, addr, &KEY)
    }

    fn start_with(
        hub: &Arc<Hub>,
        name: &str,
        incarnation: u64,
        addr: SocketAddr,
        key: &[u8; 32],
    ) -> Gossip {
        let (port, inbox) = hub.port(addr);
        let me = identity(name, incarnation, addr);
        Gossip::start(
            me,
            GossipKey::derive(key),
            settings(),
            Arc::new(port),
            inbox,
        )
    }

    fn names(g: &Gossip) -> Vec<String> {
        g.members().borrow().nodes.keys().cloned().collect()
    }

    async fn three(hub: &Arc<Hub>) -> (Gossip, Gossip, Gossip) {
        let (a, b, c) = (
            start(hub, "a", 1, at(1)),
            start(hub, "b", 1, at(2)),
            start(hub, "c", 1, at(3)),
        );
        b.announce(vec![at(1)]);
        c.announce(vec![at(1)]);
        eventually("a, b and c everywhere", || {
            [&a, &b, &c].iter().all(|g| names(g) == ["a", "b", "c"])
        })
        .await;
        (a, b, c)
    }

    #[tokio::test(start_paused = true)]
    async fn three_nodes_join_through_one_seed() {
        let hub = Hub::new();
        let t = Instant::now();
        let (a, _b, _c) = three(&hub).await;
        assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
        let m = a.members().borrow().clone();
        assert_eq!(m.me, "a");
        assert_eq!(m.peers(), 2);
        assert_eq!(m.nodes["b"].addr, at(2));
        assert_eq!(m.nodes["b"].meta.phase, Phase::Active);
        assert!(!m.nodes["b"].suspect);
    }

    #[tokio::test(start_paused = true)]
    async fn a_phase_change_reaches_every_peer() {
        let hub = Hub::new();
        let (a, b, c) = three(&hub).await;
        b.set_phase(Phase::Draining);
        let phase = |g: &Gossip| g.members().borrow().nodes.get("b").map(|m| m.meta.phase);
        eventually("b draining on a and c", || {
            phase(&a) == Some(Phase::Draining) && phase(&c) == Some(Phase::Draining)
        })
        .await;
        assert_eq!(phase(&b), Some(Phase::Draining), "its own view");
        assert_eq!(names(&a), ["a", "b", "c"], "a phase change is no departure");
    }

    /// The config generation a node applies reaches every peer (Clustering §9.1), and the
    /// newest one any member applies is what a coordinator behind reads.
    #[tokio::test(start_paused = true)]
    async fn a_config_generation_reaches_every_peer() {
        let hub = Hub::new();
        let (a, b, c) = three(&hub).await;
        assert_eq!(a.members().borrow().newest_generation(), 1);
        b.advertiser().set_config_generation(2);
        let generation = |g: &Gossip| {
            g.members()
                .borrow()
                .nodes
                .get("b")
                .map(|m| m.meta.config_generation)
        };
        eventually("b at 2 on a and c", || {
            generation(&a) == Some(2) && generation(&c) == Some(2)
        })
        .await;
        assert_eq!(generation(&b), Some(2), "its own view");
        assert_eq!(a.members().borrow().newest_generation(), 2);
        assert_eq!(
            names(&a),
            ["a", "b", "c"],
            "a new generation is no departure"
        );
    }

    /// A node's file changed under it: its new hash reaches every peer (D-M13b.3), and its API
    /// address travels with it.
    #[tokio::test(start_paused = true)]
    async fn a_file_hash_reaches_every_peer() {
        let hub = Hub::new();
        let (a, b, c) = three(&hub).await;
        let meta = |g: &Gossip| g.members().borrow().nodes.get("b").map(|m| m.meta.clone());
        assert_eq!(meta(&a).unwrap().api_url, "http://10.0.0.2:8080");
        assert!(b.advertiser().set_file_hash("fedcba9876543210".into()));
        eventually("b's new hash on a and c", || {
            [&a, &c]
                .iter()
                .all(|g| meta(g).is_some_and(|m| m.file_hash == "fedcba9876543210"))
        })
        .await;
        assert_eq!(names(&a), ["a", "b", "c"], "a new hash is no departure");
    }

    #[tokio::test(start_paused = true)]
    async fn a_leave_is_seen_at_once() {
        let hub = Hub::new();
        let (a, b, c) = three(&hub).await;
        let t = Instant::now();
        b.leave().await;
        eventually("b gone from a and c", || {
            names(&a) == ["a", "c"] && names(&c) == ["a", "c"]
        })
        .await;
        assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
    }

    /// A crash: the others suspect the node within a probe or two, and it stays a member until
    /// the suspicion runs out (4 · max(1, log10 3) · 1 s = 4 s).
    #[tokio::test(start_paused = true)]
    async fn a_killed_node_is_suspect_then_gone() {
        let hub = Hub::new();
        let (a, b, c) = three(&hub).await;
        let t = Instant::now();
        drop(b);
        let suspect = |g: &Gossip| {
            g.members()
                .borrow()
                .nodes
                .get("b")
                .is_some_and(|m| m.suspect)
        };
        eventually("b suspect on a", || suspect(&a)).await;
        let suspected = t.elapsed();
        assert_eq!(names(&a), ["a", "b", "c"], "a suspect is still a member");
        eventually("b gone from a and c", || {
            names(&a) == ["a", "c"] && names(&c) == ["a", "c"]
        })
        .await;
        let gone = t.elapsed();
        assert!(
            suspected < Duration::from_secs(4),
            "suspected after {suspected:?}"
        );
        assert!(gone >= Duration::from_secs(4), "gone after {gone:?}");
        assert!(gone < Duration::from_secs(12), "gone after {gone:?}");
    }

    /// A pod's restart: the same name, a later incarnation, another address. The new identity
    /// counts at once; the old one lingers in foca until declared down, hidden by the grouping.
    #[tokio::test(start_paused = true)]
    async fn a_restart_at_a_new_address_counts_at_once() {
        let hub = Hub::new();
        let (a, b, c) = three(&hub).await;
        drop(b);
        tokio::time::sleep(Duration::from_secs(2)).await;
        let b2 = start(&hub, "b", 2, at(4));
        let t = Instant::now();
        b2.announce(vec![at(1)]);
        let addr = |g: &Gossip| {
            g.members()
                .borrow()
                .nodes
                .get("b")
                .map(|m| (m.addr, m.suspect))
        };
        eventually("b at its new address on a and c", || {
            addr(&a) == Some((at(4), false)) && addr(&c) == Some((at(4), false))
        })
        .await;
        assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
        tokio::time::sleep(Duration::from_secs(15)).await;
        assert_eq!(
            addr(&a),
            Some((at(4), false)),
            "the old identity's death changes nothing"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_partition_splits_the_members_and_a_heal_joins_them() {
        let hub = Hub::new();
        let (a, b, c) = three(&hub).await;
        hub.cut(&[at(1), at(2)], &[at(3)]);
        eventually("{a, b} | {c}", || {
            names(&a) == ["a", "b"] && names(&b) == ["a", "b"] && names(&c) == ["c"]
        })
        .await;
        hub.heal();
        let t = Instant::now();
        eventually("healed", || {
            [&a, &b, &c].iter().all(|g| names(g) == ["a", "b", "c"])
        })
        .await;
        // An announce to the down members every push_pull_interval (5 s here).
        assert!(
            t.elapsed() < Duration::from_secs(11),
            "healed after {:?}",
            t.elapsed()
        );
    }

    /// D-M13b.8: a probe's ping and ack give its round trip. A cut makes each side suspect the
    /// other (raised); once healed, a suspicion reaches its subject (received), which refutes it
    /// or, declared down first, rejoins: which side, the dissemination decides.
    #[tokio::test(start_paused = true)]
    async fn the_figures_keep_a_probe_s_round_trip_and_suspicions_each_way() {
        let hub = Hub::new();
        let (a, b, c) = three(&hub).await;
        let metrics = Metrics::new();
        a.report_to(metrics.clone());
        eventually("a probed and was acked", || {
            let f = a.figures().borrow().clone();
            f.last_probe.is_some() && f.probe_rtt.is_some()
        })
        .await;
        hub.cut(&[at(1), at(2)], &[at(3)]);
        eventually("a or b suspects c", || {
            [&a, &b]
                .iter()
                .any(|g| g.figures().borrow().raised.iter().any(|(n, _)| n == "c"))
        })
        .await;
        hub.heal();
        eventually("a suspicion received", || {
            [&a, &b, &c]
                .iter()
                .any(|g| !g.figures().borrow().received.is_empty())
        })
        .await;
        eventually("c back", || names(&a) == ["a", "b", "c"]).await;
        let text = metrics.encode();
        assert!(
            text.contains("data_avalanche_gossip_probe_rtt_seconds_count"),
            "{text}"
        );
    }

    #[test]
    fn suspicions_older_than_a_day_are_forgotten() {
        let now = Utc::now();
        let old = now - chrono::Duration::hours(25);
        let mut f = GossipFigures {
            raised: vec![("b".into(), old), ("c".into(), now)],
            received: vec![old, now],
            ..GossipFigures::default()
        };
        f.forget_before(now);
        assert_eq!(f.raised, [("c".to_owned(), now)]);
        assert_eq!(f.received, [now]);
    }

    /// A node on another anchor has another key: its packets don't open, so it never joins.
    #[tokio::test(start_paused = true)]
    async fn a_node_with_another_key_cant_join() {
        let hub = Hub::new();
        let (a, _b, _c) = three(&hub).await;
        let d = start_with(&hub, "d", 1, at(4), &[8; 32]);
        d.announce(vec![at(1)]);
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(names(&a), ["a", "b", "c"]);
        assert_eq!(names(&d), ["d"]);
    }

    /// The real wire: two nodes on loopback.
    #[tokio::test]
    async fn two_nodes_join_over_udp() {
        async fn node(name: &str) -> Gossip {
            let any = SocketAddr::from(([127, 0, 0, 1], 0));
            let (udp, inbox) = crate::Udp::bind(&[any]).await.unwrap();
            let me = identity(name, 1, udp.local_addrs()[0]);
            Gossip::start(
                me,
                GossipKey::derive(&KEY),
                settings(),
                Arc::new(udp),
                inbox,
            )
        }
        let (a, b) = (node("a").await, node("b").await);
        b.announce(vec![a.members().borrow().nodes["a"].addr]);
        for g in [&a, &b] {
            let mut m = g.members();
            tokio::time::timeout(Duration::from_secs(10), m.wait_for(|m| m.peers() == 1))
                .await
                .expect("a and b see each other within 10 s")
                .unwrap();
        }
        assert_eq!(names(&a), ["a", "b"]);
        assert_eq!(names(&b), ["a", "b"]);
    }

    #[test]
    fn the_members_are_each_names_latest_live_identity() {
        let me = identity("a", 1, at(1));
        let old = identity("b", 1, at(2));
        let new = NodeIdentity {
            rev: 3,
            ..identity("b", 2, at(4))
        };
        let gone = identity("c", 1, at(3));
        let state = [
            (&me, State::Alive),
            (&old, State::Suspect),
            (&new, State::Alive),
            (&gone, State::Down),
        ];
        let m = members_of(&me, state.into_iter());
        assert_eq!(m.nodes.keys().collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(m.nodes["b"].addr, at(4));
        // A newer identity that is down doesn't hide an older live one (a clock that went back).
        let down_new = [(&new, State::Down), (&old, State::Suspect)];
        let m = members_of(&me, down_new.into_iter());
        assert_eq!((m.nodes["b"].addr, m.nodes["b"].suspect), (at(2), true));
    }

    /// Config §6.5.1: `retransmit_factor · ceil(log10(n + 1))` transmissions and a suspicion of
    /// `4 · max(1, log10 n) · probe_interval`.
    #[test]
    fn foca_follows_the_spec_formulas() {
        let s = settings();
        let at_size = |n| {
            let c = foca_config(&s, n);
            (c.max_transmissions.get(), c.suspect_to_down_after)
        };
        assert_eq!(at_size(1), (4, Duration::from_secs(4)));
        assert_eq!(at_size(3), (4, Duration::from_secs(4)));
        assert_eq!(at_size(10), (8, Duration::from_secs(4)));
        assert_eq!(at_size(100), (12, Duration::from_secs(8)));
        let c = foca_config(&s, 3);
        assert_eq!(
            (c.probe_period, c.probe_rtt),
            (s.probe_interval, s.probe_timeout)
        );
        let p = |p: &Option<PeriodicParams>| p.as_ref().map(|p| (p.frequency, p.num_members.get()));
        assert_eq!(p(&c.periodic_gossip), Some((Duration::from_millis(200), 3)));
        assert_eq!(p(&c.periodic_announce), Some((Duration::from_secs(5), 1)));
        assert_eq!(
            p(&c.periodic_announce_to_down_members),
            Some((Duration::from_secs(5), 2))
        );
        assert_eq!(c.max_packet_size.get(), MAX_PAYLOAD);
        // Every size's settings replace any other's (foca refuses some changes).
        let mut swim: Swim = Foca::new(identity("a", 1, at(1)), c, rand::make_rng(), PostcardCodec);
        for n in [1, 2, 50, 1000] {
            swim.set_config(foca_config(&s, n)).unwrap();
        }
    }
}
