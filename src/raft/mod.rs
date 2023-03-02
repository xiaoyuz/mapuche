use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::{middleware, App, HttpServer};
use openraft::{declare_raft_types, Config, Raft};
use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;

use crate::raft::app::MapucheRaftApp;
use crate::raft::network::raft_network_impl::MapucheNetwork;
use crate::raft::network::{api, management, raft};
use crate::raft::store::{RocksRequest, RocksResponse, RocksStore};

mod app;
mod meta;
mod network;
mod store;
mod test;

pub type MapucheNodeId = u64;

declare_raft_types!(
    /// Declare the type configuration for `MemStore`.
    pub TypeConfig: D = RocksRequest, R = RocksResponse, NodeId = MapucheNodeId, Node = MapucheNode
);

pub type MapucheRaft = Raft<TypeConfig, MapucheNetwork, Arc<RocksStore>>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct MapucheNode {
    pub rpc_addr: String,
    pub api_addr: String,
}

impl Display for MapucheNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MapucheNode {{ rpc_addr: {}, api_addr: {} }}",
            self.rpc_addr, self.api_addr
        )
    }
}

#[allow(dead_code)]
pub async fn start_example_raft_node<P>(
    node_id: MapucheNodeId,
    dir: P,
    http_addr: String,
    rcp_addr: String,
) -> std::io::Result<()>
where
    P: AsRef<Path>,
{
    // Create a configuration for the raft instance.
    let config = Config {
        heartbeat_interval: 250,
        election_timeout_min: 299,
        ..Default::default()
    };

    let config = Arc::new(config.validate().unwrap());

    // Create a instance of where the Raft data will be stored.
    let store = RocksStore::new(&dir).await;

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = MapucheNetwork {};

    // Create a local raft instance.
    let raft = Raft::new(node_id, config.clone(), network, store.clone())
        .await
        .unwrap();

    let app = Arc::new(MapucheRaftApp {
        id: node_id,
        api_addr: http_addr.clone(),
        rpc_addr: rcp_addr.clone(),
        raft,
        store,
        config,
    });

    // Create an application that will store all the instances created above, this will
    // be later used on the actix-web services.
    let web_app = Data::new(app.clone());

    // Start the actix-web server.
    let server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            .app_data(web_app.clone())
            // raft internal RPC
            .service(raft::append)
            .service(raft::snapshot)
            .service(raft::vote)
            // admin API
            .service(management::init)
            .service(management::add_learner)
            .service(management::change_membership)
            .service(management::metrics)
            // application API
            .service(api::write)
            .service(api::read)
            .service(api::consistent_read)
    });

    let x = server.bind(http_addr)?;

    x.run().await
}
