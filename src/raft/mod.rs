use actix_web::middleware::{self, Logger};
use actix_web::{App, HttpServer};
use openraft::{declare_raft_types, BasicNode, Config, Raft};
use std::path::Path;
use std::thread;
use tokio::runtime::Runtime;
use tonic::transport::Server;

use crate::Command;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::raft::app::MapucheRaftApp;
use crate::raft::network::raft_network_impl::MapucheRaftNetworkFactory;

use crate::raft::store::{RaftResponse, RaftStore};

use self::network::management;
use self::network::rpc::raft_rpc::raft_server;
use self::network::rpc::RaftRpcService;

pub mod app;
pub mod client;
pub mod network;
pub mod store;

pub type MapucheNodeId = u64;

pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<MapucheNodeId, E>;
pub type RPCError<E = openraft::error::Infallible> =
    openraft::error::RPCError<MapucheNodeId, BasicNode, RaftError<E>>;

pub type ClientWriteError = openraft::error::ClientWriteError<MapucheNodeId, BasicNode>;
pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<MapucheNodeId, BasicNode>;
pub type ForwardToLeader = openraft::error::ForwardToLeader<MapucheNodeId, BasicNode>;
pub type InitializeError = openraft::error::InitializeError<MapucheNodeId, BasicNode>;

pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;

declare_raft_types!(
    /// Declare the type configuration for `MemStore`.
    pub TypeConfig: D = RaftRequest, R = RaftResponse, NodeId = MapucheNodeId, Node = BasicNode
);

pub type MapucheRaft = Raft<TypeConfig, MapucheRaftNetworkFactory, Arc<RaftStore>>;

pub static mut RAFT_APP: Option<Arc<MapucheRaftApp>> = None;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftRequest {
    Set { key: String, value: String },
    CmdLog { id: String, cmd: Command },
}

impl From<&str> for RaftRequest {
    fn from(value: &str) -> Self {
        serde_json::from_str(value).unwrap()
    }
}

pub fn get_raft_app() -> Option<Arc<MapucheRaftApp>> {
    unsafe { RAFT_APP.clone() }
}

pub async fn start_raft_node<P>(
    node_id: MapucheNodeId,
    dir: P,
    addr: &str,
    api_addr: &str,
) -> std::io::Result<()>
where
    P: AsRef<Path>,
{
    // Create a configuration for the raft instance.
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };

    let config = Arc::new(config.validate().unwrap());

    // Create a instance of where the Raft data will be stored.
    let store = RaftStore::new(&dir).await;

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = MapucheRaftNetworkFactory {};

    // Create a local raft instance.
    let raft = Raft::new(node_id, config.clone(), network, store.clone())
        .await
        .unwrap();

    let app = MapucheRaftApp {
        id: node_id,
        addr: addr.to_string(),
        raft,
        store,
        config,
    };

    unsafe {
        RAFT_APP.replace(Arc::new(app.clone()));
    }

    let addr = addr.parse().unwrap();
    let rpc_service = RaftRpcService::default();

    thread::spawn(move || {
        Runtime::new().unwrap().block_on(async move {
            Server::builder()
                .add_service(raft_server::RaftServer::new(rpc_service))
                .serve(addr)
                .await
                .unwrap();
        })
    });

    // Start the actix-web server.
    let server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            // admin API
            .service(management::init)
            .service(management::add_learner)
            .service(management::change_membership)
            .service(management::metrics)
    });

    let x = server.bind(api_addr)?;

    x.run().await
}
