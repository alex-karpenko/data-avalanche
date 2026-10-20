//! C2 metadata update, C3 graceful leave, C4 kill → suspect → dead, C5 restart under the same name.

use std::time::{Duration, Instant};

use cluster_capability::memberlist_node::{Node, node, options, within};

async fn three() -> (Node, Node, Node) {
    let a = node("a", 0, b"a0", options()).await;
    let b = node("b", 0, b"b0", options()).await;
    let c = node("c", 0, b"c0", options()).await;
    b.join(a.addr()).await;
    c.join(a.addr()).await;
    within("converged", Duration::from_secs(10), async || {
        a.view().await.len() == 3 && b.view().await.len() == 3 && c.view().await.len() == 3
    })
    .await;
    (a, b, c)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c2_a_metadata_update_reaches_every_peer() {
    let (a, b, c) = three().await;
    let big = vec![b'x'; 512];
    *b.meta.0.lock().unwrap() = big.clone();
    let t = Instant::now();
    b.list.update_node(Duration::from_secs(5)).await.unwrap();
    let took = within(
        "b's new meta on a and c",
        Duration::from_secs(10),
        async || {
            let on = async |n: &Node| {
                n.view()
                    .await
                    .iter()
                    .any(|(id, _, m)| id == "b" && *m == big)
            };
            on(&a).await && on(&c).await
        },
    )
    .await;
    println!(
        "C2 512-byte meta update seen in {:?} (update_node {:?})",
        took,
        t.elapsed()
    );
    for n in [&a, &b, &c] {
        n.list.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c3_a_leave_is_left_not_dead() {
    let (a, b, c) = three().await;
    let t = Instant::now();
    assert!(b.list.leave(Duration::from_secs(5)).await.unwrap());
    let left = within(
        "b offline on a and c",
        Duration::from_secs(10),
        async || a.online().await == ["a", "c"] && c.online().await == ["a", "c"],
    )
    .await;
    println!(
        "C3 leave() returned in {:?}; peers dropped b from online in {left:?}",
        t.elapsed()
    );
    // The public NodeState keeps the alive message's state: Left isn't observable.
    println!("C3 public state of b on a: {:?}", a.state_of("b").await);
    b.list.shutdown().await.unwrap();
    for n in [&a, &c] {
        n.list.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c4_a_killed_node_goes_suspect_then_dead() {
    let (a, b, c) = three().await;
    let t = Instant::now();
    b.list.shutdown().await.unwrap();
    let dead = within(
        "b offline on a and c",
        Duration::from_secs(60),
        async || a.online().await == ["a", "c"] && c.online().await == ["a", "c"],
    )
    .await;
    println!(
        "C4 kill: offline on both after {dead:?} ({:?})",
        t.elapsed()
    );
    for n in [&a, &c] {
        n.list.shutdown().await.unwrap();
    }
}

/// A restart under the same name: at the same address, then at another (a pod's new IP).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c5_a_restart_under_the_same_name_rejoins() {
    let (a, b, c) = three().await;
    let port = b.addr().port();
    b.list.shutdown().await.unwrap();
    within("b offline", Duration::from_secs(60), async || {
        a.online().await == ["a", "c"]
    })
    .await;
    // Same address.
    let b2 = node("b", port, b"b1", options()).await;
    let t = Instant::now();
    b2.join(a.addr()).await;
    let took = within(
        "b alive again with b1",
        Duration::from_secs(30),
        async || {
            let on = async |n: &Node| {
                n.online().await == ["a", "b", "c"]
                    && n.view()
                        .await
                        .iter()
                        .any(|(id, _, m)| id == "b" && m == b"b1")
            };
            on(&a).await && on(&c).await
        },
    )
    .await;
    println!(
        "C5 same address: alive again in {took:?} ({:?})",
        t.elapsed()
    );
    // Another address, at once after a kill.
    b2.list.shutdown().await.unwrap();
    within("b offline again", Duration::from_secs(60), async || {
        a.online().await == ["a", "c"]
    })
    .await;
    let b3 = node("b", 0, b"b2", options()).await;
    let t = Instant::now();
    let joined = b3
        .list
        .join(memberlist::proto::MaybeResolvedAddress::Resolved(a.addr()))
        .await;
    println!("C5 new address: join -> {joined:?}");
    let r = tokio::time::timeout(
        Duration::from_secs(60),
        within(
            "b alive at its new address",
            Duration::from_secs(60),
            async || {
                a.list
                    .online_members()
                    .await
                    .iter()
                    .any(|n| n.id() == "b" && *n.address() == b3.addr())
            },
        ),
    )
    .await;
    println!("C5 new address: {r:?} after {:?}", t.elapsed());
    for n in [&a, &c, &b3] {
        n.list.shutdown().await.unwrap();
    }
}
