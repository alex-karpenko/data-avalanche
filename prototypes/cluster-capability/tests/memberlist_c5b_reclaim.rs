//! C5b: a restart at a new address under `dead_node_reclaim_time` 0 and 1 s.

use std::time::{Duration, Instant};

use cluster_capability::memberlist_node::{node, options, within};
use memberlist::proto::MaybeResolvedAddress;

async fn new_address_rejoin(reclaim: Duration) -> Duration {
    let o = || options().with_dead_node_reclaim_time(reclaim);
    let a = node("a", 0, b"a", o()).await;
    let b = node("b", 0, b"b", o()).await;
    b.join(a.addr()).await;
    within("joined", Duration::from_secs(10), async || {
        a.online().await.len() == 2
    })
    .await;
    b.list.shutdown().await.unwrap();
    within("b offline", Duration::from_secs(60), async || {
        a.online().await == ["a"]
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let b2 = node("b", 0, b"b2", o()).await;
    let t = Instant::now();
    let _ = b2.list.join(MaybeResolvedAddress::Resolved(a.addr())).await;
    let took = within(
        "b at its new address",
        Duration::from_secs(90),
        async || {
            a.list
                .online_members()
                .await
                .iter()
                .any(|n| n.id() == "b" && *n.address() == b2.addr())
        },
    )
    .await;
    for n in [&a, &b2] {
        n.list.shutdown().await.unwrap();
    }
    let _ = t;
    took
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c5b_reclaim_zero_and_one_second() {
    for r in [
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::ZERO,
        Duration::from_secs(30),
    ] {
        println!(
            "C5b reclaim {r:?}: rejoined at the new address in {:?}",
            new_address_rejoin(r).await
        );
    }
}
