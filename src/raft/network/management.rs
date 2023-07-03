// --- Cluster management

use crate::raft::{get_raft_app, MapucheNodeId};
use actix_web::web::Json;
use actix_web::{get, post, Responder};
use openraft::error::Infallible;
use openraft::{BasicNode, RaftMetrics};
use std::collections::{BTreeMap, BTreeSet};

/// Add a node as **Learner**.
///
/// A Learner receives log replication from the leader but does not vote.
/// This should be done before adding a node as a member into the cluster
/// (by calling `change-membership`)
#[post("/add-learner")]
pub async fn add_learner(req: Json<(MapucheNodeId, String)>) -> actix_web::Result<impl Responder> {
    let node_id = req.0 .0;
    let node = BasicNode {
        addr: req.0 .1.clone(),
    };
    let res = get_raft_app()
        .unwrap()
        .raft
        .add_learner(node_id, node, true)
        .await;
    Ok(Json(res))
}

/// Changes specified learners to members, or remove members.
#[post("/change-membership")]
pub async fn change_membership(
    req: Json<BTreeSet<MapucheNodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = get_raft_app()
        .unwrap()
        .raft
        .change_membership(req.0, false)
        .await;
    Ok(Json(res))
}

/// Initialize a single-node cluster.
#[post("/init")]
pub async fn init() -> actix_web::Result<impl Responder> {
    let app = get_raft_app().unwrap();
    let mut nodes = BTreeMap::new();
    nodes.insert(
        app.id,
        BasicNode {
            addr: app.addr.clone(),
        },
    );
    let res = app.raft.initialize(nodes).await;
    Ok(Json(res))
}

/// Get the latest metrics of the cluster
#[get("/metrics")]
pub async fn metrics() -> actix_web::Result<impl Responder> {
    let metrics = get_raft_app().unwrap().raft.metrics().borrow().clone();

    let res: Result<RaftMetrics<MapucheNodeId, BasicNode>, Infallible> = Ok(metrics);
    Ok(Json(res))
}
