//! F1…F8: foca 2.0 through a tokio actor, the same questions as C1…C13 for memberlist.

use std::time::Duration;

use cluster_capability::foca_node::{Hub, Node, Wire, at, config, within};
use foca::State;

const S: Duration = Duration::from_secs(1);

async fn three(hub: &std::sync::Arc<Hub>, base: u16) -> (Node, Node, Node) {
    let a = Node::start("a", 1, at(base), Wire::Hub(hub.clone()), config()).await;
    let b = Node::start("b", 1, at(base + 1), Wire::Hub(hub.clone()), config()).await;
    let c = Node::start("c", 1, at(base + 2), Wire::Hub(hub.clone()), config()).await;
    b.announce(a.id.addr);
    c.announce(a.id.addr);
    within("three active everywhere", 10 * S, || {
        [&a, &b, &c].iter().all(|n| n.active() == ["a", "b", "c"])
    })
    .await;
    (a, b, c)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f1_three_nodes_converge_over_udp() {
    let any = at(0);
    let a = Node::start("a", 1, any, Wire::Udp(dummy()), config()).await;
    let b = Node::start("b", 1, any, Wire::Udp(dummy()), config()).await;
    let c = Node::start("c", 1, any, Wire::Udp(dummy()), config()).await;
    b.announce(a.id.addr);
    c.announce(a.id.addr);
    let took = within("three active everywhere (UDP)", 10 * S, || {
        [&a, &b, &c].iter().all(|n| n.active() == ["a", "b", "c"])
    })
    .await;
    println!("F1 UDP converged in {took:?}");
    for n in [&a, &b, &c] {
        n.stop();
    }
}

fn dummy() -> std::sync::Arc<tokio::net::UdpSocket> {
    // Wire::Udp's socket is bound by Node::start; this placeholder only selects the variant.
    let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    s.set_nonblocking(true).unwrap();
    std::sync::Arc::new(tokio::net::UdpSocket::from_std(s).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f2_a_metadata_change_reaches_every_peer() {
    let hub = Hub::new();
    let (a, b, c) = three(&hub, 1000).await;
    let meta = vec![b'm'; 200];
    b.set_meta(&meta);
    let took = within("b's meta on a and c", 10 * S, || {
        a.meta_of("b") == Some(meta.clone()) && c.meta_of("b") == Some(meta.clone())
    })
    .await;
    println!(
        "F2 200-byte meta seen in {took:?}; a active {:?}",
        a.active()
    );
    assert_eq!(a.active(), ["a", "b", "c"], "a meta change is no departure");
    for n in [&a, &b, &c] {
        n.stop();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f3_a_leave_is_seen_at_once() {
    let hub = Hub::new();
    let (a, b, c) = three(&hub, 1100).await;
    b.leave();
    let took = within("b down on a and c", 10 * S, || {
        a.active() == ["a", "c"] && c.active() == ["a", "c"]
    })
    .await;
    println!("F3 leave seen in {took:?}");
    for n in [&a, &b, &c] {
        n.stop();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f4_a_killed_node_is_suspect_then_down() {
    let hub = Hub::new();
    let (a, b, c) = three(&hub, 1200).await;
    b.stop();
    let suspect = within("b suspect on a or c", 20 * S, || {
        a.state_of("b") != Some(State::Alive) || c.state_of("b") != Some(State::Alive)
    })
    .await;
    let down = within("b down on a and c", 30 * S, || {
        a.active() == ["a", "c"] && c.active() == ["a", "c"]
    })
    .await;
    println!(
        "F4 kill: suspect after {suspect:?}, down on both after {:?}",
        suspect + down
    );
    for n in [&a, &c] {
        n.stop();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f5_a_restart_at_a_new_address_replaces_the_old_identity() {
    let hub = Hub::new();
    let (a, b, c) = three(&hub, 1300).await;
    b.stop();
    // A pod's restart: same name, next incarnation, another IP, two seconds later.
    tokio::time::sleep(2 * S).await;
    let b2 = Node::start("b", 2, at(1399), Wire::Hub(hub.clone()), config()).await;
    b2.announce(a.id.addr);
    let took = within("b at incarnation 2, active on a and c", 10 * S, || {
        let on = |n: &Node| {
            n.view
                .borrow()
                .iter()
                .any(|m| m.0 == "b" && m.1 == 2 && m.4 == State::Alive)
        };
        on(&a) && on(&c)
    })
    .await;
    println!(
        "F5 restart at a new address active in {took:?}; a's view {:?}",
        a.view
            .borrow()
            .iter()
            .map(|m| (m.0.clone(), m.1, m.4))
            .collect::<Vec<_>>()
    );
    for n in [&a, &c, &b2] {
        n.stop();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f6_a_partition_and_its_heal() {
    let hub = Hub::new();
    let (a, b, c) = three(&hub, 1400).await;
    hub.cut(&[a.id.addr, b.id.addr], &[c.id.addr]);
    let split = within("{a,b} | {c}", 30 * S, || {
        a.active() == ["a", "b"] && b.active() == ["a", "b"] && c.active() == ["c"]
    })
    .await;
    println!("F6 partition seen in {split:?}");
    hub.heal();
    let healed = within("healed", 60 * S, || {
        [&a, &b, &c].iter().all(|n| n.active() == ["a", "b", "c"])
    })
    .await;
    println!(
        "F6 healed in {healed:?}; c's notes {:?}",
        c.notes
            .lock()
            .unwrap()
            .iter()
            .filter(|n| !matches!(
                n,
                foca::OwnedNotification::MemberUp(_) | foca::OwnedNotification::MemberDown(_)
            ))
            .collect::<Vec<_>>()
    );
    for n in [&a, &b, &c] {
        n.stop();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f7_a_stop_the_world_pause() {
    let hub = Hub::new();
    let (a, b, c) = three(&hub, 1500).await;
    // A short pause: no one declares b down.
    b.pause(Duration::from_millis(1500));
    tokio::time::sleep(4 * S).await;
    println!("F7 after a 1.5 s pause, a sees b {:?}", a.state_of("b"));
    // A long pause: b is declared down, then rejoins (renew) once it runs again.
    b.pause(15 * S);
    let down = within("b down on a", 30 * S, || a.active() == ["a", "c"]).await;
    let back = within("b back on a", 60 * S, || a.active() == ["a", "b", "c"]).await;
    println!(
        "F7 15 s pause: down after {down:?}, back {back:?} later; b's notes {:?}",
        b.notes
            .lock()
            .unwrap()
            .iter()
            .filter(|n| !matches!(
                n,
                foca::OwnedNotification::MemberUp(_) | foca::OwnedNotification::MemberDown(_)
            ))
            .collect::<Vec<_>>()
    );
    for n in [&a, &b, &c] {
        n.stop();
    }
}

#[test]
fn f8_an_identity_encodes_small() {
    use foca::Codec;
    let id = cluster_capability::foca_node::NodeId {
        name: "data-avalanche-2".into(),
        incarnation: 1_760_000_000_000,
        rev: 7,
        addr: "10.244.1.17:7946".parse().unwrap(),
        meta: br#"{"phase":"Active","gen":87,"build":"1.0.0+abc1234"}"#.to_vec(),
    };
    let mut buf = Vec::new();
    foca::PostcardCodec
        .encode_member(&foca::Member::alive(id), &mut buf)
        .unwrap();
    println!("F8 a member encodes to {} bytes", buf.len());
}
