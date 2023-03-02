use crate::raft::store::RocksSnapshot;
use crate::raft::MapucheNodeId;
/// Meta data of a raft-store.
///
/// In raft, except logs and state machine, the store also has to store several piece of metadata.
/// This sub mod defines the key-value pairs of these metadata.
use openraft::{ErrorSubject, LogId};
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Defines metadata key and value
pub(crate) trait StoreMeta {
    /// The key used to store in rocksdb
    const KEY: &'static str;

    /// The type of the value to store
    type Value: Serialize + DeserializeOwned;

    /// The subject this meta belongs to, and will be embedded into the returned storage error.
    fn subject(v: Option<&Self::Value>) -> ErrorSubject<MapucheNodeId>;
}

pub(crate) struct LastPurged {}
pub(crate) struct SnapshotIndex {}
pub(crate) struct Vote {}
pub(crate) struct Snapshot {}

impl StoreMeta for LastPurged {
    const KEY: &'static str = "last_purged_log_id";
    type Value = LogId<u64>;

    fn subject(_v: Option<&Self::Value>) -> ErrorSubject<MapucheNodeId> {
        ErrorSubject::Store
    }
}

impl StoreMeta for SnapshotIndex {
    const KEY: &'static str = "snapshot_index";
    type Value = u64;

    fn subject(_v: Option<&Self::Value>) -> ErrorSubject<MapucheNodeId> {
        ErrorSubject::Store
    }
}

impl StoreMeta for Vote {
    const KEY: &'static str = "vote";
    type Value = openraft::Vote<MapucheNodeId>;

    fn subject(_v: Option<&Self::Value>) -> ErrorSubject<MapucheNodeId> {
        ErrorSubject::Vote
    }
}

impl StoreMeta for Snapshot {
    const KEY: &'static str = "snapshot";
    type Value = RocksSnapshot;

    fn subject(v: Option<&Self::Value>) -> ErrorSubject<MapucheNodeId> {
        ErrorSubject::Snapshot(v.unwrap().meta.signature())
    }
}
