use crate::raft::{MapucheNodeId, TypeConfig};

use openraft::async_trait::async_trait;
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState,
    RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote,
};

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;

use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

type StorageResult<T> = Result<T, StorageError<MapucheNodeId>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RocksRequest {
    Set { key: String, value: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RocksResponse {
    pub value: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RocksSnapshot {
    pub meta: SnapshotMeta<MapucheNodeId, BasicNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/**
 * Here defines a state machine of the raft, this state represents a copy of the data
 * between each node. Note that we are using `serde` to serialize the `data`, which has
 * a implementation to be serialized. Note that for this test we set both the key and
 * value as String, but you could set any type of value that has the serialization impl.
 */
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct RocksStateMachine {
    pub last_applied_log: Option<LogId<MapucheNodeId>>,

    pub last_membership: StoredMembership<MapucheNodeId, BasicNode>,

    /// Application data.
    pub data: BTreeMap<String, String>,
}

#[derive(Debug, Default)]
pub struct RocksStore {
    last_purged_log_id: RwLock<Option<LogId<MapucheNodeId>>>,
    /// The Raft log.
    log: RwLock<BTreeMap<u64, Entry<TypeConfig>>>,
    /// The Raft state machine.
    pub state_machine: RwLock<RocksStateMachine>,
    /// The current granted vote.
    vote: RwLock<Option<Vote<MapucheNodeId>>>,
    snapshot_idx: Arc<Mutex<u64>>,
    current_snapshot: RwLock<Option<RocksSnapshot>>,
}

#[async_trait]
impl RaftLogReader<TypeConfig> for Arc<RocksStore> {
    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let log = self.log.read().await;
        let last = log.iter().rev().next().map(|(_, ent)| ent.log_id);

        let last_purged = *self.last_purged_log_id.read().await;

        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TypeConfig>>> {
        let log = self.log.read().await;
        let response = log
            .range(range.clone())
            .map(|(_, val)| val.clone())
            .collect::<Vec<_>>();
        Ok(response)
    }
}

#[async_trait]
impl RaftSnapshotBuilder<TypeConfig, Cursor<Vec<u8>>> for Arc<RocksStore> {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<MapucheNodeId, BasicNode, Cursor<Vec<u8>>>, StorageError<MapucheNodeId>>
    {
        let data;
        let last_applied_log;
        let last_membership;

        {
            // Serialize the data of the state machine.
            let state_machine = self.state_machine.read().await;
            data = serde_json::to_vec(&*state_machine).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    AnyError::new(&e),
                )
            })?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership.clone();
        }

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock().await;
            *l += 1;
            *l
        };

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = RocksSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStorage<TypeConfig> for Arc<RocksStore> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(
        &mut self,
        vote: &Vote<MapucheNodeId>,
    ) -> Result<(), StorageError<MapucheNodeId>> {
        let mut v = self.vote.write().await;
        *v = Some(*vote);
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<MapucheNodeId>>, StorageError<MapucheNodeId>> {
        Ok(*self.vote.read().await)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log(&mut self, entries: &[&Entry<TypeConfig>]) -> StorageResult<()> {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.log_id.index, (*entry).clone());
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<MapucheNodeId>,
    ) -> StorageResult<()> {
        let mut log = self.log.write().await;
        let keys = log
            .range(log_id.index..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            log.remove(&key);
        }

        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<MapucheNodeId>,
    ) -> Result<(), StorageError<MapucheNodeId>> {
        {
            let mut ld = self.last_purged_log_id.write().await;
            assert!(*ld <= Some(log_id));
            *ld = Some(log_id);
        }

        {
            let mut log = self.log.write().await;

            let keys = log
                .range(..=log_id.index)
                .map(|(k, _v)| *k)
                .collect::<Vec<_>>();
            for key in keys {
                log.remove(&key);
            }
        }

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<MapucheNodeId>>,
            StoredMembership<MapucheNodeId, BasicNode>,
        ),
        StorageError<MapucheNodeId>,
    > {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<TypeConfig>],
    ) -> Result<Vec<RocksResponse>, StorageError<MapucheNodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(RocksResponse { value: None }),
                EntryPayload::Normal(ref req) => match req {
                    RocksRequest::Set { key, value } => {
                        sm.data.insert(key.clone(), value.clone());
                        res.push(RocksResponse {
                            value: Some(value.clone()),
                        })
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(RocksResponse { value: None })
                }
            };
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotData>, StorageError<MapucheNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<MapucheNodeId, BasicNode>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<(), StorageError<MapucheNodeId>> {
        let new_snapshot = RocksSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let updated_state_machine: RocksStateMachine =
                serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.signature()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<Snapshot<MapucheNodeId, BasicNode, Self::SnapshotData>>,
        StorageError<MapucheNodeId>,
    > {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }
}
