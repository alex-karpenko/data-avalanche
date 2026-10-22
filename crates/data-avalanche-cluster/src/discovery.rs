//! How a node finds its peers (Clustering §3.5, §10.2, D-4) and the address it gives them.
//! `static` asks `memberlist.join_members`; `dns` asks every address `discovery_dns_name`
//! resolves to (a Kubernetes headless Service), at `advertise_port`. Seeds are asked at start and
//! again every [`REDISCOVER`] while the node has no peer, so a node that lost them all, or
//! started before them, finds its way back; once joined, SWIM carries the membership.
//!
//! A seed is named by the address its peer advertises: that peer's `advertise_addr` at
//! `advertise_port`. foca accepts an announce only when it is addressed to the receiver's own
//! identity, so one sent to a Service or load-balancer address, a NAT address, or a bind address
//! that isn't advertised is dropped without a word. `dns` meets the rule when each pod advertises
//! its Service A record (by default its first private IPv4) at a shared `advertise_port`.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use data_avalanche_config::{Cluster, Discovery, HostPort, Memberlist};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::gossip::Announcer;
use crate::{ClusterError, Members};

/// How often a node without peers asks its seeds again.
pub const REDISCOVER: Duration = Duration::from_secs(5);

/// Where a node's seeds come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Seeds {
    Static(Vec<HostPort>),
    Dns { name: String, port: u16 },
}

impl Seeds {
    pub fn new(cluster: &Cluster) -> Self {
        let m = &cluster.memberlist;
        match (&cluster.discovery, &cluster.discovery_dns_name) {
            (Discovery::Dns, Some(name)) => Self::Dns {
                name: name.clone(),
                port: m.advertise_port,
            },
            // V-096 refuses `dns` without a name.
            _ => Self::Static(m.join_members.clone()),
        }
    }

    /// The seeds' addresses now, without `me`'s own. A name that doesn't resolve is logged
    /// and skipped: the next round asks again.
    pub async fn resolve(&self, me: &[SocketAddr]) -> Vec<SocketAddr> {
        let names: Vec<(String, u16)> = match self {
            Self::Static(seeds) => seeds.iter().map(|s| (s.host.clone(), s.port)).collect(),
            Self::Dns { name, port } => vec![(name.clone(), *port)],
        };
        let mut addrs = Vec::new();
        for (host, port) in names {
            match tokio::net::lookup_host((host.as_str(), port)).await {
                Ok(found) => addrs.extend(found),
                Err(e) => tracing::warn!("gossip seed {host}:{port} doesn't resolve: {e}"),
            }
        }
        addrs.retain(|a| !me.contains(a));
        addrs.sort_unstable();
        addrs.dedup();
        addrs
    }
}

/// Asks the seeds at once, then every [`REDISCOVER`] while the node has no peer.
pub fn discover(
    seeds: Seeds,
    me: Vec<SocketAddr>,
    announcer: Announcer,
    members: watch::Receiver<Members>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if members.borrow().peers() == 0 {
                let addrs = seeds.resolve(&me).await;
                if !addrs.is_empty() {
                    tracing::debug!(?addrs, "asking the gossip seeds");
                    announcer.announce(addrs);
                }
            }
            tokio::time::sleep(REDISCOVER).await;
        }
    })
}

/// The address the others reach this node at: `advertise_addr` (an IP, or a name resolved
/// now), else a specific `bind_addr`, else the host's first private IPv4 among `interfaces`;
/// at `advertise_port`.
///
/// # Errors
/// `advertise_addr` doesn't resolve, or no address qualifies.
pub async fn advertise(m: &Memberlist, interfaces: &[IpAddr]) -> Result<SocketAddr, ClusterError> {
    let port = m.advertise_port;
    if !m.advertise_addr.is_empty() {
        if let Ok(ip) = m.advertise_addr.parse::<IpAddr>() {
            return Ok(SocketAddr::new(ip, port));
        }
        let found = tokio::net::lookup_host((m.advertise_addr.as_str(), port))
            .await
            .map_err(|e| {
                ClusterError::AdvertiseAddr(format!("{:?} doesn't resolve: {e}", m.advertise_addr))
            })?;
        return found.into_iter().next().ok_or_else(|| {
            ClusterError::AdvertiseAddr(format!("{:?} resolves to nothing", m.advertise_addr))
        });
    }
    if let Some(ip) = m.bind_addr.iter().find(|ip| !ip.is_unspecified()) {
        return Ok(SocketAddr::new(*ip, port));
    }
    interfaces
        .iter()
        .find(|ip| matches!(ip, IpAddr::V4(v4) if v4.is_private()))
        .map(|ip| SocketAddr::new(*ip, port))
        .ok_or_else(|| {
            ClusterError::AdvertiseAddr(
                "the gossip binds every address and the host has no private IPv4 address".into(),
            )
        })
}

/// The host's interface addresses, for [`advertise`].
pub fn interfaces() -> Vec<IpAddr> {
    match if_addrs::get_if_addrs() {
        Ok(found) => found.into_iter().map(|i| i.ip()).collect(),
        Err(e) => {
            tracing::warn!("the host's addresses can't be listed: {e}");
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;
    use crate::test_support::{config, eventually};

    fn memberlist() -> Memberlist {
        config(&["p"]).globals.cluster.memberlist
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[tokio::test]
    async fn the_advertised_address_is_the_configured_one_else_a_bind_address_else_a_private_one() {
        let ifs = [
            ip("127.0.0.1"),
            ip("203.0.113.9"),
            ip("10.1.2.3"),
            ip("192.168.0.4"),
        ];
        let mut m = memberlist();
        m.advertise_port = 7000;
        assert_eq!(m.bind_addr, [IpAddr::V4(Ipv4Addr::UNSPECIFIED)]);
        assert_eq!(
            advertise(&m, &ifs).await.unwrap(),
            "10.1.2.3:7000".parse().unwrap()
        );
        m.bind_addr = vec![ip("0.0.0.0"), ip("172.16.0.8")];
        assert_eq!(
            advertise(&m, &ifs).await.unwrap(),
            "172.16.0.8:7000".parse().unwrap()
        );
        m.advertise_addr = "10.9.9.9".into();
        assert_eq!(
            advertise(&m, &ifs).await.unwrap(),
            "10.9.9.9:7000".parse().unwrap()
        );
        m.advertise_addr = "localhost".into();
        assert!(advertise(&m, &ifs).await.unwrap().ip().is_loopback());
    }

    #[tokio::test]
    async fn no_private_address_and_no_setting_is_an_error_naming_the_key() {
        let m = memberlist();
        let e = advertise(&m, &[ip("127.0.0.1"), ip("203.0.113.9")])
            .await
            .unwrap_err();
        assert!(e.to_string().contains("advertise_addr"), "{e}");
        let mut m = memberlist();
        m.advertise_addr = "no-such-host.invalid".into();
        let e = advertise(&m, &[]).await.unwrap_err();
        assert!(e.to_string().contains("no-such-host.invalid"), "{e}");
    }

    #[tokio::test]
    async fn static_seeds_resolve_and_leave_this_node_out() {
        let seeds = Seeds::Static(vec![
            "127.0.0.1:7001".parse().unwrap(),
            "127.0.0.1:7002".parse().unwrap(),
            "no-such-host.invalid:7003".parse().unwrap(),
        ]);
        let me = ["127.0.0.1:7002".parse().unwrap()];
        assert_eq!(
            seeds.resolve(&me).await,
            ["127.0.0.1:7001".parse().unwrap()]
        );
    }

    #[tokio::test]
    async fn dns_seeds_are_every_address_at_the_advertise_port() {
        let mut c = config(&["p"]).globals.cluster;
        c.discovery = Discovery::Dns;
        c.discovery_dns_name = Some("localhost".into());
        c.memberlist.advertise_port = 7010;
        let seeds = Seeds::new(&c);
        assert_eq!(
            seeds,
            Seeds::Dns {
                name: "localhost".into(),
                port: 7010
            }
        );
        let found = seeds.resolve(&[]).await;
        assert!(!found.is_empty());
        assert!(
            found
                .iter()
                .all(|a| a.ip().is_loopback() && a.port() == 7010),
            "{found:?}"
        );
    }

    /// A node that started before its seed asks again while alone, and joins once the seed is
    /// up.
    #[tokio::test(start_paused = true)]
    async fn a_node_without_peers_asks_its_seeds_again() {
        use crate::test_support::{Hub, at, gossip_settings};
        let hub = Hub::new();
        let start = |name: &str, n: u8| {
            let (port, inbox) = hub.port(at(n));
            let me = crate::NodeIdentity {
                node_id: name.into(),
                incarnation: 1,
                rev: 0,
                addr: at(n),
                meta: crate::NodeMeta {
                    phase: crate::Phase::Active,
                    config_generation: 1,
                    build: "1.0.0".into(),
                },
            };
            let key = crate::GossipKey::derive(&[7; 32]);
            crate::Gossip::start(me, key, gossip_settings(), std::sync::Arc::new(port), inbox)
        };
        let b = start("b", 2);
        let seeds = Seeds::Static(vec![HostPort {
            host: at(1).ip().to_string(),
            port: at(1).port(),
        }]);
        let _discovery = discover(seeds, vec![at(2)], b.announcer(), b.members());
        tokio::time::sleep(Duration::from_secs(12)).await;
        let a = start("a", 1);
        let t = tokio::time::Instant::now();
        eventually("b joins a", || a.members().borrow().peers() == 1).await;
        assert!(
            t.elapsed() <= REDISCOVER + Duration::from_secs(1),
            "{:?}",
            t.elapsed()
        );
        drop(b);
    }
}
