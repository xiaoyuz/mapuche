use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::{middleware, App, HttpServer};
use openraft::{declare_raft_types, BasicNode, Config, Raft};

use std::sync::Arc;

use crate::raft::app::MapucheRaftApp;
use crate::raft::network::raft_network_impl::MapucheNetwork;
use crate::raft::network::{api, management, raft};
use crate::raft::store::{RocksRequest, RocksResponse, RocksStore};

pub mod app;
pub mod client;
pub mod network;
pub mod store;
pub mod test;

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
    pub TypeConfig: D = RocksRequest, R = RocksResponse, NodeId = MapucheNodeId, Node = BasicNode
);

pub type MapucheRaft = Raft<TypeConfig, MapucheNetwork, Arc<RocksStore>>;

#[allow(dead_code)]
pub async fn start_raft_node(
    node_id: MapucheNodeId,
    http_addr: String,
) -> std::io::Result<()> {
    // Create a configuration for the raft instance.
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };

    let config = Arc::new(config.validate().unwrap());

    // Create a instance of where the Raft data will be stored.
    let store = Arc::new(RocksStore::default());

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = MapucheNetwork {};

    // Create a local raft instance.
    let raft = Raft::new(node_id, config.clone(), network, store.clone())
        .await
        .unwrap();

    // Create an application that will store all the instances created above, this will
    // be later used on the actix-web services.
    let app = Data::new(MapucheRaftApp {
        id: node_id,
        addr: http_addr.clone(),
        raft,
        store,
        config,
    });

    // Start the actix-web server.
    let server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            .app_data(app.clone())
            // raft internal RPC
            .service(raft::append)
            .service(raft::snapshot)
            .service(raft::vote)
            // admin API
            .service(management::init)
            .service(management::add_learner)
            .service(management::change_membership)
            .service(management::metrics)
            .service(management::hello)
            // application API
            .service(api::write)
            .service(api::read)
            .service(api::consistent_read)
    });

    let x = server.bind(http_addr)?;

    x.run().await
}
