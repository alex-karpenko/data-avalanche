//! A foca node as a tokio actor over a wire: a UDP socket, or an in-memory hub that can drop
//! traffic between chosen nodes (partitions) and be paused per node (stop-the-world).

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use foca::{
    AccumulatingRuntime, Config, Foca, Identity, OwnedNotification, PostcardCodec, State, Timer,
};
use rand::rngs::StdRng;
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch};

/// A member: the node's stable name, its process incarnation, a revision bumped on every
/// metadata change (and on a rejoin after being declared down), its address and metadata.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct NodeId {
    pub name: String,
    pub incarnation: u64,
    pub rev: u32,
    pub addr: SocketAddr,
    pub meta: Vec<u8>,
}

impl Identity for NodeId {
    type Addr = SocketAddr;
    fn renew(&self) -> Option<Self> {
        Some(Self {
            rev: self.rev + 1,
            ..self.clone()
        })
    }
    fn addr(&self) -> SocketAddr {
        self.addr
    }
    fn win_addr_conflict(&self, other: &Self) -> bool {
        (self.incarnation, self.rev) > (other.incarnation, other.rev)
    }
}

/// An in-memory network: an inbox per address, and blocked (from, to) pairs.
#[derive(Default)]
pub struct Hub {
    inboxes: Mutex<HashMap<SocketAddr, mpsc::UnboundedSender<Bytes>>>,
    blocked: Mutex<HashSet<(SocketAddr, SocketAddr)>>,
}

impl Hub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    fn send(&self, from: SocketAddr, to: SocketAddr, data: Bytes) {
        if self.blocked.lock().unwrap().contains(&(from, to)) {
            return;
        }
        if let Some(tx) = self.inboxes.lock().unwrap().get(&to) {
            let _ = tx.send(data);
        }
    }
    /// Drops all traffic between `a` and `b`, both ways.
    pub fn cut(&self, a: &[SocketAddr], b: &[SocketAddr]) {
        let mut bl = self.blocked.lock().unwrap();
        for x in a {
            for y in b {
                bl.insert((*x, *y));
                bl.insert((*y, *x));
            }
        }
    }
    pub fn heal(&self) {
        self.blocked.lock().unwrap().clear();
    }
}

pub enum Wire {
    Udp(Arc<UdpSocket>),
    Hub(Arc<Hub>),
}

enum Input {
    Data(Bytes),
    Timer(Timer<NodeId>),
    Announce(SocketAddr),
    Meta(Vec<u8>),
    Leave,
    Pause(Duration),
    Stop,
}

/// What a node knows: each member's name, incarnation, revision, meta and state.
pub type View = Vec<(String, u64, u32, Vec<u8>, State)>;

pub struct Node {
    pub id: NodeId,
    tx: mpsc::UnboundedSender<Input>,
    pub view: watch::Receiver<View>,
    pub notes: Arc<Mutex<Vec<OwnedNotification<NodeId>>>>,
}

pub fn config() -> Config {
    let mut c = Config::new_lan(NonZeroU32::new(3).unwrap());
    // Announce to seeds/down members more often than the 30 s / 65 s defaults.
    c.periodic_announce_to_down_members = Some(foca::PeriodicParams {
        frequency: Duration::from_secs(5),
        num_members: std::num::NonZeroUsize::new(2).unwrap(),
    });
    c
}

impl Node {
    pub async fn start(
        name: &str,
        incarnation: u64,
        addr: SocketAddr,
        wire: Wire,
        cfg: Config,
    ) -> Node {
        let (addr, wire) = match wire {
            Wire::Udp(_) => {
                let s = Arc::new(UdpSocket::bind(addr).await.unwrap());
                (s.local_addr().unwrap(), Wire::Udp(s))
            }
            Wire::Hub(h) => (addr, Wire::Hub(h)),
        };
        let id = NodeId {
            name: name.into(),
            incarnation,
            rev: 0,
            addr,
            meta: vec![],
        };
        let (tx, mut rx) = mpsc::unbounded_channel::<Input>();
        match &wire {
            Wire::Udp(s) => {
                let (s, tx) = (Arc::clone(s), tx.clone());
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 1500];
                    while let Ok((n, _)) = s.recv_from(&mut buf).await {
                        if tx
                            .send(Input::Data(Bytes::copy_from_slice(&buf[..n])))
                            .is_err()
                        {
                            break;
                        }
                    }
                });
            }
            Wire::Hub(h) => {
                let (in_tx, mut in_rx) = mpsc::unbounded_channel();
                h.inboxes.lock().unwrap().insert(addr, in_tx);
                let tx = tx.clone();
                tokio::spawn(async move {
                    while let Some(d) = in_rx.recv().await {
                        if tx.send(Input::Data(d)).is_err() {
                            break;
                        }
                    }
                });
            }
        }
        let (view_tx, view) = watch::channel(View::new());
        let notes = Arc::new(Mutex::new(Vec::new()));
        let rng: StdRng = rand::make_rng();
        let mut foca = Foca::new(id.clone(), cfg, rng, PostcardCodec);
        let (timer_tx, notes2, me) = (tx.clone(), Arc::clone(&notes), id.clone());
        tokio::spawn(async move {
            let mut rt = AccumulatingRuntime::new();
            let _ = &me;
            while let Some(input) = rx.recv().await {
                let r = match input {
                    Input::Data(d) => foca.handle_data(&d, &mut rt),
                    Input::Timer(t) => foca.handle_timer(t, &mut rt),
                    Input::Announce(to) => {
                        // A seed: only its address is known; the receiver ignores the header's dst.
                        let seed = NodeId {
                            name: format!("seed:{to}"),
                            incarnation: 0,
                            rev: 0,
                            addr: to,
                            meta: vec![],
                        };
                        foca.announce(seed, &mut rt)
                    }
                    Input::Meta(m) => {
                        let next = NodeId {
                            rev: foca.identity().rev + 1,
                            meta: m,
                            ..foca.identity().clone()
                        };
                        foca.change_identity(next, &mut rt)
                    }
                    Input::Leave => foca.leave_cluster(&mut rt),
                    Input::Pause(d) => {
                        tokio::time::sleep(d).await;
                        Ok(())
                    }
                    Input::Stop => break,
                };
                let _ = r;
                while let Some((to, data)) = rt.to_send() {
                    match &wire {
                        Wire::Udp(s) => {
                            let _ = s.send_to(&data, to.addr).await;
                        }
                        Wire::Hub(h) => h.send(addr, to.addr, data),
                    }
                }
                while let Some((after, t)) = rt.to_schedule() {
                    let tx = timer_tx.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(after).await;
                        let _ = tx.send(Input::Timer(t));
                    });
                }
                while let Some(n) = rt.to_notify() {
                    notes2.lock().unwrap().push(n);
                }
                let v: View = foca
                    .iter_membership_state()
                    .map(|m| {
                        let i = m.id();
                        (
                            i.name.clone(),
                            i.incarnation,
                            i.rev,
                            i.meta.clone(),
                            m.state(),
                        )
                    })
                    .collect();
                view_tx.send_if_modified(|old| {
                    if *old != v {
                        *old = v;
                        true
                    } else {
                        false
                    }
                });
            }
            if let Wire::Hub(h) = &wire {
                h.inboxes.lock().unwrap().remove(&addr);
            }
        });
        Node {
            id: NodeId { addr, ..id },
            tx,
            view,
            notes,
        }
    }

    pub fn announce(&self, to: SocketAddr) {
        let _ = self.tx.send(Input::Announce(to));
    }
    pub fn set_meta(&self, m: &[u8]) {
        let _ = self.tx.send(Input::Meta(m.to_vec()));
    }
    pub fn leave(&self) {
        let _ = self.tx.send(Input::Leave);
    }
    pub fn pause(&self, d: Duration) {
        let _ = self.tx.send(Input::Pause(d));
    }
    pub fn stop(&self) {
        let _ = self.tx.send(Input::Stop);
    }

    /// Names this node counts active (alive or suspect), sorted, itself included.
    pub fn active(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .view
            .borrow()
            .iter()
            .filter(|m| m.4 != State::Down)
            .map(|m| m.0.clone())
            .collect();
        v.push(self.id.name.clone());
        v.sort();
        v.dedup();
        v
    }

    pub fn state_of(&self, name: &str) -> Option<State> {
        // The latest identity under that name.
        self.view
            .borrow()
            .iter()
            .filter(|m| m.0 == name)
            .max_by_key(|m| (m.1, m.2))
            .map(|m| m.4)
    }

    pub fn meta_of(&self, name: &str) -> Option<Vec<u8>> {
        self.view
            .borrow()
            .iter()
            .filter(|m| m.0 == name && m.4 != State::Down)
            .max_by_key(|m| (m.1, m.2))
            .map(|m| m.3.clone())
    }
}

/// `127.0.0.1:port` for the hub.
pub fn at(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

/// Polls `f` every 20 ms until it holds; the time it took, or a panic after `max`.
pub async fn within(what: &str, max: Duration, mut f: impl FnMut() -> bool) -> Duration {
    let t = tokio::time::Instant::now();
    loop {
        if f() {
            return t.elapsed();
        }
        assert!(t.elapsed() < max, "{what}: not within {max:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
