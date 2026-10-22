//! Gossip packets (Q-M8-2). Each packet is foca's bytes followed by a keyed BLAKE3 MAC whose key
//! is derived from the cluster anchor's fingerprint key: every node that can read `globals.state`
//! has it, no config carries it, and a packet without a valid MAC is dropped. The packets aren't
//! encrypted; they carry node names, addresses and phases. The MAC authenticates the cluster's
//! packets but doesn't stop a replay of captured ones: at worst a replay churns membership
//! (foca's incarnations), and the leases keep placement safe; within Q-M8-2's threat model
//! (another cluster or stray traffic, not an on-path attacker). They travel over UDP; the
//! in-process tests carry them over a hub instead.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::ClusterError;

/// The MAC's length: a full BLAKE3 output.
pub const MAC_LEN: usize = 32;

/// The largest payload foca writes (`max_packet_size`): with the MAC, a packet fits a 1500-byte
/// MTU under IPv6 and UDP headers.
pub const MAX_PAYLOAD: usize = 1400;

/// The BLAKE3 key-derivation context of the gossip MAC key.
const CONTEXT: &str = "data-avalanche 2026-10-20 gossip packet MAC v1";

/// The key gossip packets are authenticated with.
#[derive(Clone, PartialEq, Eq)]
pub struct GossipKey([u8; 32]);

impl fmt::Debug for GossipKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GossipKey(..)")
    }
}

impl GossipKey {
    /// The key of the cluster whose anchor holds `fingerprint_key`.
    pub fn derive(fingerprint_key: &[u8; 32]) -> Self {
        Self(blake3::derive_key(CONTEXT, fingerprint_key))
    }

    /// `payload` with its MAC appended.
    pub fn seal(&self, payload: &[u8]) -> Bytes {
        let mut packet = BytesMut::with_capacity(payload.len() + MAC_LEN);
        packet.extend_from_slice(payload);
        packet.extend_from_slice(blake3::keyed_hash(&self.0, payload).as_bytes());
        packet.freeze()
    }

    /// The payload of a packet whose MAC is valid; `None` otherwise. The comparison takes the
    /// same time wherever the MACs differ (`blake3::Hash`'s equality).
    pub fn open<'a>(&self, packet: &'a [u8]) -> Option<&'a [u8]> {
        let at = packet.len().checked_sub(MAC_LEN)?;
        let (payload, mac) = packet.split_at(at);
        let mac: [u8; MAC_LEN] = mac.try_into().ok()?;
        (blake3::keyed_hash(&self.0, payload) == blake3::Hash::from_bytes(mac)).then_some(payload)
    }
}

/// Where the gossip sends its packets. A send never waits: a packet that can't go at once is
/// dropped, as UDP drops it, and SWIM's retries cover the loss.
pub trait Wire: Send + Sync + fmt::Debug + 'static {
    fn send(&self, to: SocketAddr, packet: Bytes);
}

/// How many received packets may wait for the gossip; more are dropped, as a full socket buffer
/// drops them.
pub const INBOX: usize = 1024;

/// The gossip's UDP sockets, one per bind address. A reader per socket puts every packet it
/// receives in the inbox; a writer per socket sends the packets queued for it, and ends once the
/// queue is closed and empty, so the packets of a leave still go out after the gossip has ended.
/// A packet goes out through the first socket of the destination's address family.
#[derive(Debug)]
pub struct Udp {
    sockets: Vec<(SocketAddr, mpsc::Sender<(SocketAddr, Bytes)>)>,
    readers: Vec<JoinHandle<()>>,
}

impl Udp {
    /// Binds `addrs`; the packets they receive go to the returned inbox.
    ///
    /// # Errors
    /// An address can't be bound.
    pub async fn bind(addrs: &[SocketAddr]) -> Result<(Self, mpsc::Receiver<Bytes>), ClusterError> {
        let (inbox, rx) = mpsc::channel(INBOX);
        let (mut sockets, mut readers) = (Vec::new(), Vec::new());
        for addr in addrs {
            let bind = |source| ClusterError::Bind {
                addr: *addr,
                source,
            };
            let socket = Arc::new(UdpSocket::bind(addr).await.map_err(bind)?);
            let (out, mut queued) = mpsc::channel::<(SocketAddr, Bytes)>(INBOX);
            sockets.push((socket.local_addr().map_err(bind)?, out));
            let s = Arc::clone(&socket);
            tokio::spawn(async move {
                while let Some((to, packet)) = queued.recv().await {
                    if let Err(e) = s.send_to(&packet, to).await {
                        tracing::debug!("gossip send to {to}: {e}");
                    }
                }
            });
            let inbox = inbox.clone();
            readers.push(tokio::spawn(async move {
                let mut buf = vec![0u8; MAX_PAYLOAD + MAC_LEN + 1];
                loop {
                    match socket.recv_from(&mut buf).await {
                        // A full inbox drops the packet; a closed one ends the reader.
                        Ok((n, _)) => {
                            if let Err(mpsc::error::TrySendError::Closed(_)) =
                                inbox.try_send(Bytes::copy_from_slice(&buf[..n]))
                            {
                                return;
                            }
                        }
                        Err(e) => tracing::debug!("gossip socket: {e}"),
                    }
                }
            }));
        }
        Ok((Self { sockets, readers }, rx))
    }

    /// The bound addresses (a bind to port 0 shows the port chosen).
    pub fn local_addrs(&self) -> Vec<SocketAddr> {
        self.sockets.iter().map(|(a, _)| *a).collect()
    }
}

impl Drop for Udp {
    fn drop(&mut self) {
        for r in &self.readers {
            r.abort();
        }
    }
}

impl Wire for Udp {
    fn send(&self, to: SocketAddr, packet: Bytes) {
        let socket = self
            .sockets
            .iter()
            .find(|(a, _)| a.is_ipv4() == to.is_ipv4());
        if let Some((_, out)) = socket {
            let _ = out.try_send((to, packet));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_sealed_packet_opens_and_a_changed_one_doesnt() {
        let key = GossipKey::derive(&[7; 32]);
        let packet = key.seal(b"ping");
        assert_eq!(packet.len(), 4 + MAC_LEN);
        assert_eq!(key.open(&packet), Some(&b"ping"[..]));
        for at in [0, 3, 4, packet.len() - 1] {
            let mut bad = packet.to_vec();
            bad[at] ^= 1;
            assert_eq!(key.open(&bad), None, "a flipped byte at {at}");
        }
        assert_eq!(key.open(&packet[..MAC_LEN - 1]), None, "shorter than a MAC");
        assert_eq!(key.open(&key.seal(b"")), Some(&b""[..]), "an empty payload");
    }

    /// Another cluster's anchor gives another key: its packets don't open here.
    #[test]
    fn another_anchor_is_another_key() {
        let ours = GossipKey::derive(&[7; 32]);
        let theirs = GossipKey::derive(&[8; 32]);
        assert_ne!(ours, theirs);
        assert_eq!(ours.open(&theirs.seal(b"ping")), None);
        assert_eq!(format!("{ours:?}"), "GossipKey(..)");
    }

    #[tokio::test]
    async fn an_address_taken_is_an_error_naming_it() {
        let (a, _in) = Udp::bind(&[SocketAddr::from(([127, 0, 0, 1], 0))])
            .await
            .unwrap();
        let taken = a.local_addrs()[0];
        let e = Udp::bind(&[taken]).await.unwrap_err();
        assert!(e.to_string().contains(&taken.to_string()), "{e}");
    }

    #[tokio::test]
    async fn packets_cross_udp_on_loopback() {
        let any = SocketAddr::from(([127, 0, 0, 1], 0));
        let (a, _a_in) = Udp::bind(&[any]).await.unwrap();
        let (b, mut b_in) = Udp::bind(&[any]).await.unwrap();
        let to = b.local_addrs()[0];
        assert_ne!(to.port(), 0);
        a.send(to, Bytes::from_static(b"hello"));
        let got = tokio::time::timeout(Duration::from_secs(5), b_in.recv())
            .await
            .expect("a packet within 5 s");
        assert_eq!(got.as_deref(), Some(&b"hello"[..]));
    }
}
