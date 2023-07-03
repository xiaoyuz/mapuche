use crate::raft::{
    CheckIsLeaderError, ClientWriteError, ClientWriteResponse, ForwardToLeader, InitializeError,
    MapucheNodeId, RPCError, RaftError, RaftRequest,
};
use openraft::error::{NetworkError, RemoteError};
use openraft::{BasicNode, RaftMetrics};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout;

use super::network::raft_network_impl::MapucheRaftNetworkFactory;
use super::network::rpc::{RpcReqMessage, RpcRespMessage};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Empty {}

pub struct RaftClient {
    /// The leader node to send request to.
    ///
    /// All traffic should be sent to the leader in a cluster.
    pub leader: Arc<Mutex<(MapucheNodeId, String)>>,

    pub inner: Client,
}

impl RaftClient {
    /// Create a client with a leader node id and a node manager to get node address by node id.
    pub fn new(leader_id: MapucheNodeId, leader_addr: String) -> Self {
        Self {
            leader: Arc::new(Mutex::new((leader_id, leader_addr))),
            inner: Client::new(),
        }
    }

    // --- Application API

    /// Submit a write request to the raft cluster.
    ///
    /// The request will be processed by raft protocol: it will be replicated to a quorum and then
    /// will be applied to state machine.
    ///
    /// The result of applying the request will be returned.
    pub async fn write(
        &self,
        req: &RaftRequest,
    ) -> Result<ClientWriteResponse, RPCError<ClientWriteError>> {
        let req = RpcReqMessage::Write(req.clone());
        self.write_to_leader(&req).await
    }

    /// Read value by key, in an inconsistent mode.
    ///
    /// This method may return stale value because it does not force to read on a legal leader.
    pub async fn read(&self, req: &str) -> Result<String, RPCError> {
        let req = RpcReqMessage::Read(req.to_owned());
        let res = self.do_send_rpc_to_leader(&req).await?;
        let leader_id = self.leader.lock().await.0;

        if let RpcRespMessage::Read(r) = res {
            Ok(r)
        } else {
            Err(RPCError::RemoteError(RemoteError::new(
                leader_id,
                RaftError::Fatal(openraft::error::Fatal::Panicked),
            )))
        }
    }

    /// Consistent Read value by key, in an inconsistent mode.
    ///
    /// This method MUST return consitent value or CheckIsLeaderError.
    pub async fn consistent_read(&self, req: &str) -> Result<String, RPCError<CheckIsLeaderError>> {
        let req = RpcReqMessage::ConsistentRead(req.to_owned());
        let res = self.do_send_rpc_to_leader(&req).await?;
        let leader_id = self.leader.lock().await.0;

        if let RpcRespMessage::ConsistentRead(r) = res {
            Ok(r)
        } else {
            Err(RPCError::RemoteError(RemoteError::new(
                leader_id,
                RaftError::Fatal(openraft::error::Fatal::Panicked),
            )))
        }
    }

    // --- Cluster management API

    /// Initialize a cluster of only the node that receives this request.
    ///
    /// This is the first step to initialize a cluster.
    /// With a initialized cluster, new node can be added with [`write`].
    /// Then setup replication with [`add_learner`].
    /// Then make the new node a member with [`change_membership`].
    pub async fn init(&self) -> Result<(), RPCError<InitializeError>> {
        let req = RpcReqMessage::Initialize;
        let res = self.do_send_rpc_to_leader(&req).await?;
        let leader_id = self.leader.lock().await.0;

        if let RpcRespMessage::Initialize(r) = res {
            r.map_err(|e| RPCError::RemoteError(RemoteError::new(leader_id, e)))
        } else {
            Err(RPCError::RemoteError(RemoteError::new(
                leader_id,
                RaftError::Fatal(openraft::error::Fatal::Panicked),
            )))
        }
    }

    /// Add a node as learner.
    ///
    /// The node to add has to exist, i.e., being added with `write(ExampleRequest::AddNode{})`
    pub async fn add_learner(
        &self,
        req: (MapucheNodeId, String),
    ) -> Result<ClientWriteResponse, RPCError<ClientWriteError>> {
        let req = RpcReqMessage::AddLearner(req.0, req.1);
        self.write_to_leader(&req).await
    }

    /// Change membership to the specified set of nodes.
    ///
    /// All nodes in `req` have to be already added as learner with [`add_learner`],
    /// or an error [`LearnerNotFound`] will be returned.
    pub async fn change_membership(
        &self,
        req: &BTreeSet<MapucheNodeId>,
    ) -> Result<ClientWriteResponse, RPCError<ClientWriteError>> {
        let req = RpcReqMessage::ChangeMembership(req.clone());
        self.write_to_leader(&req).await
    }

    /// Get the metrics about the cluster.
    ///
    /// Metrics contains various information about the cluster, such as current leader,
    /// membership config, replication status etc.
    /// See [`RaftMetrics`].
    pub async fn metrics(&self) -> Result<RaftMetrics<MapucheNodeId, BasicNode>, RPCError> {
        let req = RpcReqMessage::Metrics;
        let res = self.do_send_rpc_to_leader(&req).await?;
        let leader_id = self.leader.lock().await.0;

        if let RpcRespMessage::Metrics(r) = res {
            r.map_err(|_| {
                RPCError::RemoteError(RemoteError::new(
                    leader_id,
                    RaftError::Fatal(openraft::error::Fatal::Panicked),
                ))
            })
        } else {
            Err(RPCError::RemoteError(RemoteError::new(
                leader_id,
                RaftError::Fatal(openraft::error::Fatal::Panicked),
            )))
        }
    }

    async fn do_send_rpc_to_leader<Err>(
        &self,
        req: &RpcReqMessage,
    ) -> Result<RpcRespMessage, RPCError<Err>>
    where
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        let fact = MapucheRaftNetworkFactory {};
        let (_, addr) = {
            let t = self.leader.lock().await;
            (t.0, t.1.clone())
        };
        let target_node = BasicNode { addr };
        let fu = fact.send_rpc(req, &target_node);

        match timeout(Duration::from_millis(3_000), fu).await {
            Ok(x) => x.map_err(|e| RPCError::Network(NetworkError::new(&e))),
            Err(timeout_err) => Err(RPCError::Network(NetworkError::new(&timeout_err))),
        }
    }

    /// Try the best to send a request to the leader.
    ///
    /// If the target node is not a leader, a `ForwardToLeader` error will be
    /// returned and this client will retry at most 3 times to contact the updated leader.
    async fn write_to_leader(
        &self,
        req: &RpcReqMessage,
    ) -> Result<ClientWriteResponse, RPCError<ClientWriteError>> {
        // Retry at most 3 times to find a valid leader.
        let mut n_retry = 3;

        loop {
            let res = self.do_send_rpc_to_leader(req).await?;

            let psa = match res {
                RpcRespMessage::Write(r) => r,
                RpcRespMessage::AddLearner(r) => r,
                RpcRespMessage::ChangeMembership(r) => r,
                _ => Err(RaftError::Fatal(openraft::error::Fatal::Panicked)),
            };

            let rpc_err = match psa {
                Ok(x) => return Ok(x),
                Err(rpc_err) => rpc_err,
            };

            if let Some(ForwardToLeader {
                leader_id: Some(leader_id),
                leader_node: Some(leader_node),
            }) = rpc_err.forward_to_leader()
            {
                // Update target to the new leader.
                {
                    let mut t = self.leader.lock().await;
                    *t = (*leader_id, leader_node.addr.clone());
                }

                n_retry -= 1;
                if n_retry > 0 {
                    continue;
                }
            }
            return Err(RPCError::Network(NetworkError::new(&rpc_err)));
        }
    }
}
