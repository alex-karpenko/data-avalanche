//! A UDP forwarder in front of a node's gossip port (Q-M8-4). The node advertises the
//! forwarder's port, so every gossip packet to it passes here; the gossip replies to the
//! address a node advertises, never to a packet's source, so forwarding one way is enough. A
//! packet's source port is the sending node's own gossip port: dropping chosen sources cuts
//! those nodes off from this one, a partition no process notices but by its silence.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tokio::net::UdpSocket;
use tokio::task::AbortHandle;

/// The forwarder, forwarding until dropped.
#[derive(Debug)]
pub struct Forwarder {
    port: u16,
    blocked: Arc<Mutex<HashSet<u16>>>,
    task: AbortHandle,
}

impl Forwarder {
    /// Forwards what `127.0.0.1:<port>` receives to `127.0.0.1:<to>`.
    pub async fn start(to: u16) -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("a forwarder port");
        let port = socket.local_addr().expect("its address").port();
        let blocked: Arc<Mutex<HashSet<u16>>> = Arc::default();
        let b = Arc::clone(&blocked);
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            loop {
                let Ok((n, from)) = socket.recv_from(&mut buf).await else {
                    continue;
                };
                if b.lock().unwrap().contains(&from.port()) {
                    continue;
                }
                let _ = socket.send_to(&buf[..n], ("127.0.0.1", to)).await;
            }
        })
        .abort_handle();
        Self {
            port,
            blocked,
            task,
        }
    }

    /// The port the node advertises.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Drops the packets the nodes gossiping from `ports` send to this one.
    pub fn block(&self, ports: &[u16]) {
        self.blocked.lock().unwrap().extend(ports);
    }

    pub fn heal(&self) {
        self.blocked.lock().unwrap().clear();
    }
}

impl Drop for Forwarder {
    fn drop(&mut self) {
        self.task.abort();
    }
}
