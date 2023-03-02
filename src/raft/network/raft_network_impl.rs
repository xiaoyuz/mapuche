use crate::raft::{MapucheNode, MapucheNodeId, TypeConfig};
use async_trait::async_trait;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{RaftNetwork, RaftNetworkFactory};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub struct MapucheNetwork {}

impl MapucheNetwork {
    pub async fn send_rpc<Req, Resp, Err>(
        &self,
        target: MapucheNodeId,
        target_node: &MapucheNode,
        uri: &str,
        req: Req,
    ) -> Result<Resp, RPCError<MapucheNodeId, MapucheNode, Err>>
    where
        Req: Serialize,
        Err: std::error::Error + DeserializeOwned,
        Resp: DeserializeOwned,
    {
        let addr = &target_node.api_addr;
        let url = format!("http://{}/{}", addr, uri);
        let client = reqwest::Client::new();
        let resp = client
            .post(url)
            .json(&req)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let res: Result<Resp, Err> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        res.map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)))
    }
}

#[async_trait]
impl RaftNetworkFactory<TypeConfig> for MapucheNetwork {
    type Network = MapucheNetworkConnection;

    async fn new_client(&mut self, target: MapucheNodeId, node: &MapucheNode) -> Self::Network {
        MapucheNetworkConnection {
            owner: MapucheNetwork {},
            target,
            target_node: node.clone(),
        }
    }
}

pub struct MapucheNetworkConnection {
    owner: MapucheNetwork,
    target: MapucheNodeId,
    target_node: MapucheNode,
}

#[async_trait]
impl RaftNetwork<TypeConfig> for MapucheNetworkConnection {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<
        AppendEntriesResponse<MapucheNodeId>,
        RPCError<MapucheNodeId, MapucheNode, RaftError<MapucheNodeId>>,
    > {
        self.owner
            .send_rpc(self.target, &self.target_node, "raft-append", req)
            .await
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        InstallSnapshotResponse<MapucheNodeId>,
        RPCError<MapucheNodeId, MapucheNode, RaftError<MapucheNodeId, InstallSnapshotError>>,
    > {
        self.owner
            .send_rpc(self.target, &self.target_node, "raft-snapshot", req)
            .await
    }

    async fn send_vote(
        &mut self,
        req: VoteRequest<MapucheNodeId>,
    ) -> Result<
        VoteResponse<MapucheNodeId>,
        RPCError<MapucheNodeId, MapucheNode, RaftError<MapucheNodeId>>,
    > {
        self.owner
            .send_rpc(self.target, &self.target_node, "raft-vote", req)
            .await
    }
}
