//! memberlist 0.8.5: a node over TCP/UDP on loopback with a metadata delegate and an
//! event subscriber.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use memberlist::delegate::{
    CompositeDelegate, EventSubscriber, NodeDelegate, SubscribleEventDelegate,
};
use memberlist::net::{NetTransport, NetTransportOptions};
use memberlist::proto::{MaybeResolvedAddress, Meta, State};
use memberlist::tokio::{TokioRuntime, TokioSocketAddrResolver, TokioTcp};
use memberlist::{Memberlist, Options};
use smol_str::SmolStr;

pub type Id = SmolStr;
pub type Transport = NetTransport<Id, TokioSocketAddrResolver, TokioTcp, TokioRuntime>;

/// The node's metadata, settable at run time.
#[derive(Debug, Clone, Default)]
pub struct MetaCell(pub Arc<Mutex<Vec<u8>>>);

impl NodeDelegate for MetaCell {
    async fn node_meta(&self, limit: usize) -> Meta {
        let bytes = self.0.lock().unwrap().clone();
        assert!(bytes.len() <= limit, "meta {} > limit {limit}", bytes.len());
        Meta::try_from(bytes).expect("meta within the limit")
    }
}

pub type Delegate = CompositeDelegate<
    Id,
    SocketAddr,
    memberlist::delegate::VoidDelegate<Id, SocketAddr>,
    memberlist::delegate::VoidDelegate<Id, SocketAddr>,
    SubscribleEventDelegate<Id, SocketAddr>,
    memberlist::delegate::VoidDelegate<Id, SocketAddr>,
    MetaCell,
>;

pub struct Node {
    pub list: Memberlist<Transport, Delegate>,
    pub events: EventSubscriber<Id, SocketAddr>,
    pub meta: MetaCell,
}

/// SWIM timers close to config §6.5.1's defaults (probe 1 s, timeout 500 ms, gossip 200 ms).
pub fn options() -> Options {
    Options::lan()
        .with_probe_interval(Duration::from_secs(1))
        .with_probe_timeout(Duration::from_millis(500))
        .with_gossip_interval(Duration::from_millis(200))
        .with_dead_node_reclaim_time(Duration::from_secs(30))
}

/// A node named `name` on `127.0.0.1:port` (0 = any).
pub async fn node(name: &str, port: u16, meta: &[u8], opts: Options) -> Node {
    let mut t = NetTransportOptions::<Id, TokioSocketAddrResolver, TokioTcp>::new(name.into());
    t.add_bind_address(SocketAddr::from(([127, 0, 0, 1], port)));
    let cell = MetaCell(Arc::new(Mutex::new(meta.to_vec())));
    let (ev, events) = SubscribleEventDelegate::unbounded();
    let d = CompositeDelegate::new()
        .with_event_delegate(ev)
        .with_node_delegate(cell.clone());
    let list = Memberlist::with_delegate(d, t, opts)
        .await
        .expect("memberlist");
    Node {
        list,
        events,
        meta: cell,
    }
}

impl Node {
    pub fn addr(&self) -> SocketAddr {
        *self.list.advertise_address()
    }

    pub async fn join(&self, to: SocketAddr) {
        self.list
            .join(MaybeResolvedAddress::Resolved(to))
            .await
            .expect("join");
    }

    /// Every member this node knows, with its state and metadata.
    pub async fn view(&self) -> Vec<(String, State, Vec<u8>)> {
        let mut v: Vec<_> = self
            .list
            .members()
            .await
            .iter()
            .map(|n| (n.id().to_string(), n.state(), n.meta().to_vec()))
            .collect();
        v.sort_by(|x, y| x.0.cmp(&y.0));
        v
    }

    /// The members memberlist counts online (alive or suspect; dead and left are not), sorted.
    pub async fn online(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .list
            .online_members()
            .await
            .iter()
            .map(|n| n.id().to_string())
            .collect();
        v.sort();
        v
    }

    pub async fn state_of(&self, name: &str) -> Option<State> {
        self.view()
            .await
            .into_iter()
            .find(|(n, ..)| n == name)
            .map(|(_, s, _)| s)
    }
}

/// Polls `f` every 50 ms until it holds; returns the time it took, or panics after `max`.
pub async fn within<F: AsyncFnMut() -> bool>(what: &str, max: Duration, mut f: F) -> Duration {
    let t = Instant::now();
    loop {
        if f().await {
            return t.elapsed();
        }
        assert!(t.elapsed() < max, "{what}: not within {max:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
