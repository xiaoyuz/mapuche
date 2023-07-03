use std::collections::{BTreeMap, BTreeSet};

use crate::raft::network::rpc::raft_rpc::{raft_server, RaftReq, RaftResp};
use crate::raft::{get_raft_app, MapucheNodeId, RaftRequest, TypeConfig};

use openraft::error::{
    ClientWriteError, Infallible, InitializeError, InstallSnapshotError, RaftError,
};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftMetrics};
use serde::{Deserialize, Serialize};
use tonic::{Code, Request, Response, Status};

pub mod raft_rpc {
    tonic::include_proto!("raftrpc");
}

#[derive(Clone, Serialize, Deserialize)]
pub enum RpcReqMessage {
    Unknown,
    Vote(VoteRequest<MapucheNodeId>),
    Append(AppendEntriesRequest<TypeConfig>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),

    Write(RaftRequest),
    Read(String),
    ConsistentRead(String),

    AddLearner(MapucheNodeId, String),
    ChangeMembership(BTreeSet<MapucheNodeId>),
    Initialize,
    Metrics,
}

impl From<&RaftReq> for RpcReqMessage {
    fn from(value: &RaftReq) -> Self {
        serde_json::from_str(value.req.as_str()).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RpcRespMessage {
    Unknown,
    Vote(Result<VoteResponse<MapucheNodeId>, RaftError<MapucheNodeId>>),
    Append(Result<AppendEntriesResponse<MapucheNodeId>, RaftError<MapucheNodeId>>),
    InstallSnapshot(
        Result<
            InstallSnapshotResponse<MapucheNodeId>,
            RaftError<MapucheNodeId, InstallSnapshotError>,
        >,
    ),

    Write(
        Result<
            ClientWriteResponse<TypeConfig>,
            RaftError<MapucheNodeId, ClientWriteError<MapucheNodeId, BasicNode>>,
        >,
    ),
    Read(String),
    ConsistentRead(String),

    AddLearner(
        Result<
            ClientWriteResponse<TypeConfig>,
            RaftError<MapucheNodeId, ClientWriteError<MapucheNodeId, BasicNode>>,
        >,
    ),
    ChangeMembership(
        Result<
            ClientWriteResponse<TypeConfig>,
            RaftError<MapucheNodeId, ClientWriteError<MapucheNodeId, BasicNode>>,
        >,
    ),
    Initialize(Result<(), RaftError<MapucheNodeId, InitializeError<MapucheNodeId, BasicNode>>>),
    Metrics(Result<RaftMetrics<MapucheNodeId, BasicNode>, Infallible>),
}

impl From<&RpcRespMessage> for RaftResp {
    fn from(value: &RpcRespMessage) -> Self {
        let resp = serde_json::to_string(value).unwrap_or_default();
        RaftResp { resp }
    }
}

impl From<&RaftResp> for RpcRespMessage {
    fn from(value: &RaftResp) -> Self {
        serde_json::from_str(&value.resp).unwrap()
    }
}

impl RpcReqMessage {
    pub async fn handle(&self) -> Result<Response<RaftResp>, Status> {
        let app = get_raft_app().ok_or(Status::new(Code::InvalidArgument, "raft not inited"))?;
        let resp = match &self {
            Self::Vote(req) => RpcRespMessage::Vote(app.raft.vote(req.clone()).await),
            Self::Append(req) => RpcRespMessage::Append(app.raft.append_entries(req.clone()).await),
            Self::InstallSnapshot(req) => {
                RpcRespMessage::InstallSnapshot(app.raft.install_snapshot(req.clone()).await)
            }

            Self::Write(req) => RpcRespMessage::Write(app.raft.client_write(req.clone()).await),
            Self::Read(req) => {
                let state_machine = app.store.state_machine.read().await;
                let value = state_machine
                    .get(req)
                    .unwrap_or_default()
                    .unwrap_or_default();
                RpcRespMessage::Read(value)
            }
            Self::ConsistentRead(req) => {
                let ret = app.raft.is_leader().await;

                match ret {
                    Ok(_) => {
                        let state_machine = app.store.state_machine.read().await;
                        let value = state_machine
                            .get(req)
                            .unwrap_or_default()
                            .unwrap_or_default();
                        RpcRespMessage::ConsistentRead(value)
                    }
                    Err(_e) => RpcRespMessage::Unknown,
                }
            }

            Self::AddLearner(node_id, addr) => {
                let node = BasicNode { addr: addr.clone() };
                RpcRespMessage::AddLearner(app.raft.add_learner(*node_id, node, true).await)
            }
            Self::ChangeMembership(req) => RpcRespMessage::ChangeMembership(
                app.raft.change_membership(req.clone(), false).await,
            ),
            Self::Initialize => {
                let mut nodes = BTreeMap::new();
                nodes.insert(
                    app.id,
                    BasicNode {
                        addr: app.addr.clone(),
                    },
                );
                RpcRespMessage::Initialize(app.raft.initialize(nodes).await)
            }
            Self::Metrics => {
                let metrics = app.raft.metrics().borrow().clone();
                RpcRespMessage::Metrics(Ok(metrics))
            }

            _ => RpcRespMessage::Unknown,
        };
        let resp: RaftResp = (&resp).into();
        Ok(Response::new(resp))
    }
}

#[derive(Default)]
pub struct RaftRpcService {}

#[tonic::async_trait]
impl raft_server::Raft for RaftRpcService {
    async fn request(&self, request: Request<RaftReq>) -> Result<Response<RaftResp>, Status> {
        let request = request.into_inner();
        let req_message: RpcReqMessage = (&request).into();
        req_message.handle().await
    }
}
