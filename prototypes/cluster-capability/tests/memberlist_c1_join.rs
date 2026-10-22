//! C1: three nodes join through one seed and converge; each sees the others' metadata.

use std::time::Duration;

use cluster_capability::memberlist_node::{node, options, within};
use memberlist::proto::State;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c1_three_nodes_converge_with_metadata() {
    let a = node("a", 0, br#"{"v":1,"phase":"Active"}"#, options()).await;
    let b = node("b", 0, br#"{"v":1,"phase":"Joining"}"#, options()).await;
    let c = node("c", 0, b"c-meta", options()).await;
    b.join(a.addr()).await;
    c.join(a.addr()).await;
    let took = within(
        "three alive on every node",
        Duration::from_secs(10),
        async || {
            for n in [&a, &b, &c] {
                let v = n.view().await;
                if v.len() != 3 || v.iter().any(|(_, s, _)| *s != State::Alive) {
                    return false;
                }
            }
            true
        },
    )
    .await;
    println!("C1 converged in {took:?}");
    let v = c.view().await;
    assert_eq!(
        v[0],
        (
            "a".into(),
            State::Alive,
            br#"{"v":1,"phase":"Active"}"#.to_vec()
        )
    );
    assert_eq!(v[1].2, br#"{"v":1,"phase":"Joining"}"#.to_vec());
    for n in [&a, &b, &c] {
        n.list.shutdown().await.unwrap();
    }
}
