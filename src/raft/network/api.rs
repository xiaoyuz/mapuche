use crate::raft::app::MapucheRaftApp;
use crate::raft::{MapucheNodeId, RaftRequest};
use actix_web::web::{Data, Json};
use actix_web::{post, Responder};
use openraft::error::{CheckIsLeaderError, Infallible, RaftError};
use openraft::BasicNode;

/**
 * Application API
 *
 * This is where you place your application, you can use the example below to create your
 * API. The current implementation:
 *
 *  - `POST - /write` saves a value in a key and sync the nodes.
 *  - `POST - /read` attempt to find a value from a given key.
 */
#[post("/write")]
pub async fn write(
    app: Data<MapucheRaftApp>,
    req: Json<RaftRequest>,
) -> actix_web::Result<impl Responder> {
    let response = app.raft.client_write(req.0).await;
    Ok(Json(response))
}

#[post("/read")]
pub async fn read(
    app: Data<MapucheRaftApp>,
    req: Json<String>,
) -> actix_web::Result<impl Responder> {
    let state_machine = app.store.state_machine.read().await;
    let key = req.0;
    let value = state_machine.get(&key).unwrap_or_default();

    let res: Result<String, Infallible> = Ok(value.unwrap_or_default());
    Ok(Json(res))
}

#[post("/consistent_read")]
pub async fn consistent_read(
    app: Data<MapucheRaftApp>,
    req: Json<String>,
) -> actix_web::Result<impl Responder> {
    let ret = app.raft.is_leader().await;

    match ret {
        Ok(_) => {
            let state_machine = app.store.state_machine.read().await;
            let key = req.0;
            let value = state_machine.get(&key).unwrap_or_default();

            let res: Result<
                String,
                RaftError<MapucheNodeId, CheckIsLeaderError<MapucheNodeId, BasicNode>>,
            > = Ok(value.unwrap_or_default());
            Ok(Json(res))
        }
        Err(e) => Ok(Json(Err(e))),
    }
}
