use std::collections::HashMap;
use std::sync::Arc;

use crate::raft::{MapucheNodeId, TypeConfig};

use async_trait::async_trait;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};

use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tonic::Status;

use super::rpc::raft_rpc::raft_client::RaftClient;
use super::rpc::raft_rpc::RaftReq;
use super::rpc::{RpcReqMessage, RpcRespMessage};

pub mod raft_rpc {
    tonic::include_proto!("raftrpc");
}

#[derive(Clone, Default)]
pub struct MapucheRaftNetworkFactory {
    channel_map: Arc<Mutex<HashMap<String, Arc<Channel>>>>,
}

#[async_trait]
impl RaftNetworkFactory<TypeConfig> for MapucheRaftNetworkFactory {
    type Network = MapucheNetwork;

    async fn new_client(&mut self, target: MapucheNodeId, node: &BasicNode) -> Self::Network {
        MapucheNetwork {
            owner: MapucheRaftNetworkFactory::default(),
            target,
            target_node: node.clone(),
        }
    }
}

impl MapucheRaftNetworkFactory {
    pub async fn send_rpc(
        &self,
        req: &RpcReqMessage,
        target_node: &BasicNode,
    ) -> Result<RpcRespMessage, Status> {
        let addr = format!("http://{}", target_node.addr.clone());
        let mut client = self
            .get_channel(&addr)
            .await
            .map(|channel| RaftClient::new((*channel).clone()))
            .map_err(|e| Status::from_error(Box::new(e)))?;
        let req = serde_json::to_string(req).unwrap();
        let request: tonic::Request<RaftReq> = tonic::Request::new(RaftReq { req });

        client.request(request).await.map(|r| {
            let resp: RpcRespMessage = (&r.into_inner()).into();
            resp
        })
    }

    async fn get_channel(&self, addr: &str) -> Result<Arc<Channel>, tonic::transport::Error> {
        let mut map = self.channel_map.lock().await;
        if map.contains_key(addr) {
            return Ok(map[addr].clone());
        }
        let channel = Endpoint::from_shared(addr.to_owned())?.connect().await?;
        let channel = Arc::new(channel);
        map.insert(addr.to_owned(), channel.clone());
        Ok(channel)
    }
}

pub struct MapucheNetwork {
    owner: MapucheRaftNetworkFactory,
    target: MapucheNodeId,
    target_node: BasicNode,
}

#[async_trait]
impl RaftNetwork<TypeConfig> for MapucheNetwork {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<
        AppendEntriesResponse<MapucheNodeId>,
        RPCError<MapucheNodeId, BasicNode, RaftError<MapucheNodeId>>,
    > {
        let req = RpcReqMessage::Append(req);
        let res = self
            .owner
            .send_rpc(&req, &self.target_node)
            .await
            .map_err(|_| {
                RPCError::RemoteError(RemoteError::new(
                    self.target,
                    RaftError::Fatal(openraft::error::Fatal::Panicked),
                ))
            })?;
        if let RpcRespMessage::Append(r) = res {
            r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
        } else {
            Err(RPCError::RemoteError(RemoteError::new(
                self.target,
                RaftError::Fatal(openraft::error::Fatal::Panicked),
            )))
        }
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        InstallSnapshotResponse<MapucheNodeId>,
        RPCError<MapucheNodeId, BasicNode, RaftError<MapucheNodeId, InstallSnapshotError>>,
    > {
        let req = RpcReqMessage::InstallSnapshot(req);
        let res = self
            .owner
            .send_rpc(&req, &self.target_node)
            .await
            .map_err(|_| {
                RPCError::RemoteError(RemoteError::new(
                    self.target,
                    RaftError::Fatal(openraft::error::Fatal::Panicked),
                ))
            })?;
        if let RpcRespMessage::InstallSnapshot(r) = res {
            r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
        } else {
            Err(RPCError::RemoteError(RemoteError::new(
                self.target,
                RaftError::Fatal(openraft::error::Fatal::Panicked),
            )))
        }
    }

    async fn send_vote(
        &mut self,
        req: VoteRequest<MapucheNodeId>,
    ) -> Result<
        VoteResponse<MapucheNodeId>,
        RPCError<MapucheNodeId, BasicNode, RaftError<MapucheNodeId>>,
    > {
        let req = RpcReqMessage::Vote(req);
        let res = self
            .owner
            .send_rpc(&req, &self.target_node)
            .await
            .map_err(|_| {
                RPCError::RemoteError(RemoteError::new(
                    self.target,
                    RaftError::Fatal(openraft::error::Fatal::Panicked),
                ))
            })?;
        if let RpcRespMessage::Vote(r) = res {
            r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
        } else {
            Err(RPCError::RemoteError(RemoteError::new(
                self.target,
                RaftError::Fatal(openraft::error::Fatal::Panicked),
            )))
        }
    }
}
