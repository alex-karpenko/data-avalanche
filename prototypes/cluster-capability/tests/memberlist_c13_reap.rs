//! C13: after dead nodes are reaped, does `node_map` (id → index) still match `nodes`?
//! `by_id` reads through `node_map`; `members` iterates `nodes`.

use std::time::Duration;

use cluster_capability::memberlist_node::{Node, node, options, within};

fn o() -> memberlist::Options {
    // Reap dead nodes 2 s after their death instead of 30 s, and probe often.
    options().with_gossip_to_the_dead_time(Duration::from_secs(2))
}

async fn consistent(n: &Node) -> Result<(), String> {
    for m in n.list.members().await.iter() {
        match n.list.by_id(m.id()).await {
            Some(s) if s.id() == m.id() => {}
            Some(s) => return Err(format!("by_id({}) returned {}", m.id(), s.id())),
            None => return Err(format!("by_id({}) is None", m.id())),
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c13_node_map_after_reaping() {
    let mut bad = 0;
    for round in 0..6 {
        let a = node("a", 0, b"a", o()).await;
        let mut others = Vec::new();
        for i in 0..5 {
            let n = node(&format!("n{round}-{i}"), 0, b"n", o()).await;
            n.join(a.addr()).await;
            others.push(n);
        }
        within("6 online", Duration::from_secs(10), async || {
            a.online().await.len() == 6
        })
        .await;
        // Kill three at once; the reap after their gossip-to-the-dead time swaps them out.
        for n in others.drain(0..3) {
            n.list.shutdown().await.unwrap();
        }
        within("3 online", Duration::from_secs(60), async || {
            a.online().await.len() == 3
        })
        .await;
        // Wait past the reap (it runs when the probe index wraps around the node list).
        let mut found = None;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if let Err(e) = consistent(&a).await {
                found = Some(e);
                break;
            }
        }
        println!(
            "C13 round {round}: members {} {:?}",
            a.list.members().await.len(),
            found
        );
        if found.is_some() {
            bad += 1;
        }
        for n in std::iter::once(&a).chain(others.iter()) {
            n.list.shutdown().await.unwrap();
        }
    }
    println!("C13: {bad}/6 rounds with node_map out of step with nodes");
}
