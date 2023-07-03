use crate::raft::{MapucheNodeId, MapucheRaft, RaftStore};
use openraft::Config;
use std::sync::Arc;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
#[derive(Clone)]
pub struct MapucheRaftApp {
    pub id: MapucheNodeId,
    pub addr: String,
    pub raft: MapucheRaft,
    pub store: Arc<RaftStore>,
    pub config: Arc<Config>,
}
