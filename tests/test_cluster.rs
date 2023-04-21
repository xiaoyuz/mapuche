use maplit::{btreemap, btreeset};
use mapuche::raft::client::RaftClient;
use mapuche::raft::{start_raft_node, RaftRequest};
use openraft::BasicNode;
use std::collections::BTreeMap;
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;

/// Setup a cluster of 3 nodes.
/// Write to it and read from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster() -> anyhow::Result<()> {
    let get_addr = |node_id| {
        let addr = match node_id {
            1 => "127.0.0.1:21001".to_string(),
            2 => "127.0.0.1:21002".to_string(),
            3 => "127.0.0.1:21003".to_string(),
            4 => "127.0.0.1:21004".to_string(),
            _ => {
                return Err(anyhow::anyhow!("node {} not found", node_id));
            }
        };
        Ok(addr)
    };

    // --- Start 3 raft node in 3 threads.
    let d1 = tempfile::TempDir::new()?;
    let d2 = tempfile::TempDir::new()?;
    let d3 = tempfile::TempDir::new()?;
    let d4 = tempfile::TempDir::new()?;

    let _h1 = thread::spawn(|| {
        let rt = Runtime::new().unwrap();
        let x = rt.block_on(async move {
            start_raft_node(1, d1.path(), "127.0.0.1:21001".to_string()).await
        });
        println!("x: {:?}", x);
    });

    let _h2 = thread::spawn(|| {
        let rt = Runtime::new().unwrap();
        let x = rt.block_on(async move {
            start_raft_node(2, d2.path(), "127.0.0.1:21002".to_string()).await
        });
        println!("x: {:?}", x);
    });

    let _h3 = thread::spawn(|| {
        let rt = Runtime::new().unwrap();
        let x = rt.block_on(async move {
            start_raft_node(3, d3.path(), "127.0.0.1:21003".to_string()).await
        });
        println!("x: {:?}", x);
    });

    let _h4 = thread::spawn(|| {
        let rt = Runtime::new().unwrap();
        let x = rt.block_on(async move {
            start_raft_node(3, d4.path(), "127.0.0.1:21004".to_string()).await
        });
        println!("x: {:?}", x);
    });

    // Wait for server to start up.
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // --- Create a client to the first node, as a control handle to the cluster.

    let client = RaftClient::new(1, get_addr(1)?);
    let client2 = RaftClient::new(2, get_addr(2)?);
    let client3 = RaftClient::new(3, get_addr(3)?);

    // --- 1. Initialize the target node as a cluster of only one node.
    //        After init(), the single node cluster will be fully functional.

    println!("=== init single node cluster");
    client.init().await?;

    println!("=== metrics after init");
    let _x = client.metrics().await?;

    // --- 2. Add node 2 and 3 to the cluster as `Learner`, to let them start to receive log replication
    // from the        leader.

    println!("=== add-learner 2");
    let _x = client.add_learner((2, get_addr(2)?)).await?;

    println!("=== add-learner 3");
    let _x = client.add_learner((3, get_addr(3)?)).await?;

    println!("=== metrics after add-learner");
    let x = client.metrics().await?;

    assert_eq!(
        &vec![btreeset![1]],
        x.membership_config.membership().get_joint_config()
    );

    let nodes_in_cluster = x
        .membership_config
        .nodes()
        .map(|(nid, node)| (*nid, node.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        btreemap! {
            1 => BasicNode::new("127.0.0.1:21001"),
            2 => BasicNode::new("127.0.0.1:21002"),
            3 => BasicNode::new("127.0.0.1:21003"),
        },
        nodes_in_cluster
    );

    // --- 3. Turn the two learners to members. A member node can vote or elect itself as leader.

    println!("=== change-membership to 1,2,3");
    let _x = client.change_membership(&btreeset! {1,2,3}).await?;

    // --- After change-membership, some cluster state will be seen in the metrics.
    //
    // ```text
    // metrics: RaftMetrics {
    //   current_leader: Some(1),
    //   membership_config: EffectiveMembership {
    //        log_id: LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 8 },
    //        membership: Membership { learners: {}, configs: [{1, 2, 3}] }
    //   },
    //   leader_metrics: Some(LeaderMetrics { replication: {
    //     2: ReplicationMetrics { matched: Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 7 }) },
    //     3: ReplicationMetrics { matched: Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 8 }) }} })
    // }
    // ```

    println!("=== metrics after change-member");
    let x = client.metrics().await?;
    assert_eq!(
        &vec![btreeset![1, 2, 3]],
        x.membership_config.membership().get_joint_config()
    );

    // --- Try to write some application data through the leader.

    println!("=== write `foo=bar` `boo=bar`");
    let _x = client
        .write(&RaftRequest::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        })
        .await?;
    let _x = client
        .write(&RaftRequest::Set {
            key: "boo".to_string(),
            value: "bar".to_string(),
        })
        .await?;

    // --- Wait for a while to let the replication get done.

    tokio::time::sleep(Duration::from_millis(1_000)).await;

    // --- Read it on every node.

    println!("=== read `foo` on node 1");
    let x = client.read(&("foo".to_string())).await?;
    assert_eq!("bar", x);

    println!("=== read `foo` on node 2");
    let x = client2.read(&("foo".to_string())).await?;
    assert_eq!("bar", x);

    println!("=== read `foo` on node 3");
    let x = client3.read(&("foo".to_string())).await?;
    assert_eq!("bar", x);

    // --- A write to non-leader will be automatically forwarded to a known leader

    println!("=== read `foo` on node 2");
    let _x = client2
        .write(&RaftRequest::Set {
            key: "foo".to_string(),
            value: "wow".to_string(),
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(1_000)).await;

    // --- Read it on every node.

    println!("=== read `foo` on node 1");
    let x = client.read(&("foo".to_string())).await?;
    assert_eq!("wow", x);

    println!("=== read `foo` on node 2");
    let client2 = RaftClient::new(2, get_addr(2)?);
    let x = client2.read(&("foo".to_string())).await?;
    assert_eq!("wow", x);

    println!("=== read `foo` on node 3");
    let client3 = RaftClient::new(3, get_addr(3)?);
    let x = client3.read(&("foo".to_string())).await?;
    assert_eq!("wow", x);

    println!("=== consistent_read `foo` on node 1");
    let x = client.consistent_read(&("foo".to_string())).await?;
    assert_eq!("wow", x);

    println!("=== consistent_read `foo` on node 2 MUST return CheckIsLeaderError");
    let x = client2.consistent_read(&("foo".to_string())).await;
    match x {
        Err(e) => {
            let s = e.to_string();
            let expect_err:String = "error occur on remote peer 2: has to forward request to: Some(1), Some(BasicNode { addr: \"127.0.0.1:21001\" })".to_string();

            assert_eq!(s, expect_err);
        }
        Ok(_) => panic!("MUST return CheckIsLeaderError"),
    }

    println!("=== add-learner 4");
    let _x = client.add_learner((4, get_addr(4)?)).await?;

    println!("=== metrics after add-learner 4");
    let x = client.metrics().await?;
    let nodes_in_cluster = x
        .membership_config
        .nodes()
        .map(|(nid, node)| (*nid, node.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        btreemap! {
            1 => BasicNode::new("127.0.0.1:21001"),
            2 => BasicNode::new("127.0.0.1:21002"),
            3 => BasicNode::new("127.0.0.1:21003"),
            4 => BasicNode::new("127.0.0.1:21004"),
        },
        nodes_in_cluster
    );
    println!("=== change-membership to 1,2,3,4");
    let _x = client.change_membership(&btreeset! {1,2,3,4}).await?;
    let x = client.metrics().await?;
    assert_eq!(
        &vec![btreeset![1, 2, 3, 4]],
        x.membership_config.membership().get_joint_config()
    );
    println!("=== read `foo` on node 4");
    let client4 = RaftClient::new(4, get_addr(4)?);
    let x = client4.read(&("foo".to_string())).await?;
    assert_eq!("wow", x);
    let x = client4.read(&("boo".to_string())).await?;
    assert_eq!("bar", x);

    // --- Remove node 1 from the cluster.

    println!("=== change-membership to 2,3,4 ");
    let _x = client.change_membership(&btreeset! {2,3,4}).await?;

    tokio::time::sleep(Duration::from_millis(60_000)).await;

    println!("=== metrics after change-membership to {{2,3,4}}");
    let x = client.metrics().await?;
    assert_eq!(
        &vec![btreeset![2, 3, 4]],
        x.membership_config.membership().get_joint_config()
    );

    println!("=== write `foo=zoo` to node-3");
    let _x = client3
        .write(&RaftRequest::Set {
            key: "foo".to_string(),
            value: "zoo".to_string(),
        })
        .await?;

    println!("=== read `foo=zoo` to node-3");
    let got = client3.read(&"foo".to_string()).await?;
    assert_eq!("zoo", got);
    Ok(())
}
