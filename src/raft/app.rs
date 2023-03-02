use crate::raft::{MapucheNodeId, MapucheRaft, RocksStore};
use openraft::Config;
use std::sync::Arc;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct MapucheRaftApp {
    pub id: MapucheNodeId,
    pub api_addr: String,
    pub rpc_addr: String,
    pub raft: MapucheRaft,
    pub store: Arc<RocksStore>,
    pub config: Arc<Config>,
}
