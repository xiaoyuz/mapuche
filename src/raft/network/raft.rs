// --- Raft communication

use crate::raft::app::MapucheRaftApp;
use crate::raft::{MapucheNodeId, TypeConfig};
use actix_web::web::{Data, Json};
use actix_web::{post, Responder};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};

#[post("/raft-vote")]
pub async fn vote(
    app: Data<MapucheRaftApp>,
    req: Json<VoteRequest<MapucheNodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.vote(req.0).await;
    Ok(Json(res))
}

#[post("/raft-append")]
pub async fn append(
    app: Data<MapucheRaftApp>,
    req: Json<AppendEntriesRequest<TypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.append_entries(req.0).await;
    Ok(Json(res))
}

#[post("/raft-snapshot")]
pub async fn snapshot(
    app: Data<MapucheRaftApp>,
    req: Json<InstallSnapshotRequest<TypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.install_snapshot(req.0).await;
    Ok(Json(res))
}
