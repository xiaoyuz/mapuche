use crate::raft::{MapucheNodeId, RaftRequest, TypeConfig};

use openraft::async_trait::async_trait;
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState,
    RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote,
};

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error::Error;

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, Direction, Options, DB};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::Frame;

type StorageResult<T> = Result<T, StorageError<MapucheNodeId>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftResponse {
    OptString(Option<String>),
    Frame(Frame),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RaftSnapshot {
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
pub struct SerializableRaftStateMachine {
    pub last_applied_log: Option<LogId<MapucheNodeId>>,

    pub last_membership: StoredMembership<MapucheNodeId, BasicNode>,

    /// Application data.
    pub data: BTreeMap<String, String>,
}

impl From<&RaftStateMachine> for SerializableRaftStateMachine {
    fn from(state: &RaftStateMachine) -> Self {
        let mut data = BTreeMap::new();
        for res in state.db.iterator_cf(
            &state.db.cf_handle("data").expect("cf_handle"),
            rocksdb::IteratorMode::Start,
        ) {
            let (key, value) = res.unwrap();
            let key: &[u8] = &key;
            let value: &[u8] = &value;
            data.insert(
                String::from_utf8(key.to_vec()).expect("invalid key"),
                String::from_utf8(value.to_vec()).expect("invalid data"),
            );
        }
        Self {
            last_applied_log: state.get_last_applied_log().expect("last_applied_log"),
            last_membership: state.get_last_membership().expect("last_membership"),
            data,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RaftStateMachine {
    /// Application data.
    pub db: Arc<DB>,
}

impl RaftStateMachine {
    fn get_last_membership(&self) -> StorageResult<StoredMembership<MapucheNodeId, BasicNode>> {
        self.db
            .get_cf(
                &self.db.cf_handle("state_machine").expect("cf_handle"),
                "last_membership".as_bytes(),
            )
            .map_err(sm_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .unwrap_or_else(|| Ok(StoredMembership::default()))
            })
    }

    fn set_last_membership(
        &self,
        membership: StoredMembership<MapucheNodeId, BasicNode>,
    ) -> StorageResult<()> {
        self.db
            .put_cf(
                &self.db.cf_handle("state_machine").expect("cf_handle"),
                "last_membership".as_bytes(),
                serde_json::to_vec(&membership).map_err(sm_w_err)?,
            )
            .map_err(sm_w_err)
    }

    fn get_last_applied_log(&self) -> StorageResult<Option<LogId<MapucheNodeId>>> {
        self.db
            .get_cf(
                &self.db.cf_handle("state_machine").expect("cf_handle"),
                "last_applied_log".as_bytes(),
            )
            .map_err(sm_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .transpose()
            })
    }

    fn set_last_applied_log(&self, log_id: LogId<MapucheNodeId>) -> StorageResult<()> {
        self.db
            .put_cf(
                &self.db.cf_handle("state_machine").expect("cf_handle"),
                "last_applied_log".as_bytes(),
                serde_json::to_vec(&log_id).map_err(sm_w_err)?,
            )
            .map_err(sm_w_err)
    }

    fn from_serializable(
        sm: SerializableRaftStateMachine,
        db: Arc<rocksdb::DB>,
    ) -> StorageResult<Self> {
        for (key, value) in sm.data {
            db.put_cf(
                &db.cf_handle("data").unwrap(),
                key.as_bytes(),
                value.as_bytes(),
            )
            .map_err(sm_w_err)?;
        }
        let r = Self { db };
        if let Some(log_id) = sm.last_applied_log {
            r.set_last_applied_log(log_id)?;
        }
        r.set_last_membership(sm.last_membership)?;

        Ok(r)
    }

    fn new(db: Arc<rocksdb::DB>) -> RaftStateMachine {
        Self { db }
    }

    fn insert(&self, key: String, value: String) -> StorageResult<()> {
        self.db
            .put_cf(
                &self.db.cf_handle("data").unwrap(),
                key.as_bytes(),
                value.as_bytes(),
            )
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into()
            })
    }

    pub fn get(&self, key: &str) -> StorageResult<Option<String>> {
        let key = key.as_bytes();
        self.db
            .get_cf(&self.db.cf_handle("data").unwrap(), key)
            .map(|value| {
                value.map(|value| String::from_utf8(value.to_vec()).expect("invalid data"))
            })
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
            })
    }
}

fn sm_r_err<E: Error + 'static>(e: E) -> StorageError<MapucheNodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Read,
        AnyError::new(&e),
    )
    .into()
}
fn sm_w_err<E: Error + 'static>(e: E) -> StorageError<MapucheNodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Write,
        AnyError::new(&e),
    )
    .into()
}

#[derive(Debug)]
pub struct RaftStore {
    db: Arc<DB>,
    /// The Raft state machine.
    pub state_machine: RwLock<RaftStateMachine>,
}

impl RaftStore {
    pub(crate) async fn new<P: AsRef<Path>>(db_path: P) -> Arc<RaftStore> {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let store = ColumnFamilyDescriptor::new("store", Options::default());
        let state_machine = ColumnFamilyDescriptor::new("state_machine", Options::default());
        let data = ColumnFamilyDescriptor::new("data", Options::default());
        let logs = ColumnFamilyDescriptor::new("logs", Options::default());

        let db = DB::open_cf_descriptors(&db_opts, db_path, vec![store, state_machine, data, logs])
            .unwrap();

        let db = Arc::new(db);
        let state_machine = RwLock::new(RaftStateMachine::new(db.clone()));
        Arc::new(RaftStore { db, state_machine })
    }
}

impl RaftStore {
    fn store(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle("store").unwrap()
    }

    fn logs(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle("logs").unwrap()
    }

    fn flush(
        &self,
        subject: ErrorSubject<MapucheNodeId>,
        verb: ErrorVerb,
    ) -> Result<(), StorageIOError<MapucheNodeId>> {
        self.db
            .flush_wal(true)
            .map_err(|e| StorageIOError::new(subject, verb, AnyError::new(&e)))?;
        Ok(())
    }

    fn get_last_purged(&self) -> StorageResult<Option<LogId<u64>>> {
        Ok(self
            .db
            .get_cf(&self.store(), b"last_purged_log_id")
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e))
            })?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn set_last_purged(&self, log_id: LogId<u64>) -> StorageResult<()> {
        self.db
            .put_cf(
                &self.store(),
                b"last_purged_log_id",
                serde_json::to_vec(&log_id).unwrap().as_slice(),
            )
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e))
            })?;

        self.flush(ErrorSubject::Store, ErrorVerb::Write)?;
        Ok(())
    }

    fn get_snapshot_index(&self) -> StorageResult<u64> {
        Ok(self
            .db
            .get_cf(&self.store(), b"snapshot_index")
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e))
            })?
            .and_then(|v| serde_json::from_slice(&v).ok())
            .unwrap_or(0))
    }

    fn set_snapshot_index(&self, snapshot_index: u64) -> StorageResult<()> {
        self.db
            .put_cf(
                &self.store(),
                b"snapshot_index",
                serde_json::to_vec(&snapshot_index).unwrap().as_slice(),
            )
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Store,
                    ErrorVerb::Write,
                    AnyError::new(&e),
                ),
            })?;
        self.flush(ErrorSubject::Store, ErrorVerb::Write)?;
        Ok(())
    }

    fn set_vote(&self, vote: &Vote<MapucheNodeId>) -> StorageResult<()> {
        self.db
            .put_cf(&self.store(), b"vote", serde_json::to_vec(vote).unwrap())
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Vote,
                    ErrorVerb::Write,
                    AnyError::new(&e),
                ),
            })?;

        self.flush(ErrorSubject::Vote, ErrorVerb::Write)?;
        Ok(())
    }

    fn get_vote(&self) -> StorageResult<Option<Vote<MapucheNodeId>>> {
        Ok(self
            .db
            .get_cf(&self.store(), b"vote")
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Vote,
                    ErrorVerb::Write,
                    AnyError::new(&e),
                ),
            })?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn get_current_snapshot(&self) -> StorageResult<Option<RaftSnapshot>> {
        Ok(self
            .db
            .get_cf(&self.store(), b"snapshot")
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Store,
                    ErrorVerb::Read,
                    AnyError::new(&e),
                ),
            })?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn set_current_snapshot(&self, snap: RaftSnapshot) -> StorageResult<()> {
        self.db
            .put_cf(
                &self.store(),
                b"snapshot",
                serde_json::to_vec(&snap).unwrap().as_slice(),
            )
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Snapshot(snap.meta.signature()),
                    ErrorVerb::Write,
                    AnyError::new(&e),
                ),
            })?;
        self.flush(
            ErrorSubject::Snapshot(snap.meta.signature()),
            ErrorVerb::Write,
        )?;
        Ok(())
    }
}

/// converts an id to a byte vector for storing in the database.
/// Note that we're using big endian encoding to ensure correct sorting of keys
fn id_to_bin(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id).unwrap();
    buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

#[async_trait]
impl RaftLogReader<TypeConfig> for Arc<RaftStore> {
    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let last = self
            .db
            .iterator_cf(&self.logs(), rocksdb::IteratorMode::End)
            .next()
            .and_then(|res| {
                let (_, ent) = res.unwrap();
                Some(
                    serde_json::from_slice::<Entry<TypeConfig>>(&ent)
                        .ok()?
                        .log_id,
                )
            });

        let last_purged_log_id = self.get_last_purged()?;

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TypeConfig>>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        };
        self.db
            .iterator_cf(
                &self.logs(),
                rocksdb::IteratorMode::From(&start, Direction::Forward),
            )
            .map(|res| {
                let (id, val) = res.unwrap();
                let entry: StorageResult<Entry<_>> =
                    serde_json::from_slice(&val).map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Logs,
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    });
                let id = bin_to_id(&id);

                assert_eq!(Ok(id), entry.as_ref().map(|e| e.log_id.index));
                (id, entry)
            })
            .take_while(|(id, _)| range.contains(id))
            .map(|x| x.1)
            .collect()
    }
}

#[async_trait]
impl RaftSnapshotBuilder<TypeConfig, Cursor<Vec<u8>>> for Arc<RaftStore> {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<MapucheNodeId, BasicNode, Cursor<Vec<u8>>>, StorageError<MapucheNodeId>>
    {
        let data;
        let last_applied_log;
        let last_membership;

        {
            // Serialize the data of the state machine.
            let state_machine =
                SerializableRaftStateMachine::from(&*self.state_machine.read().await);
            data = serde_json::to_vec(&state_machine).map_err(sm_r_err)?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership;
        }

        // TODO: we probably want this to be atomic.
        let snapshot_idx: u64 = self.get_snapshot_index()? + 1;
        self.set_snapshot_index(snapshot_idx)?;

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

        let snapshot = RaftSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        self.set_current_snapshot(snapshot)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStorage<TypeConfig> for Arc<RaftStore> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(
        &mut self,
        vote: &Vote<MapucheNodeId>,
    ) -> Result<(), StorageError<MapucheNodeId>> {
        self.set_vote(vote)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<MapucheNodeId>>, StorageError<MapucheNodeId>> {
        self.get_vote()
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log(&mut self, entries: &[&Entry<TypeConfig>]) -> StorageResult<()> {
        for entry in entries {
            let id = id_to_bin(entry.log_id.index);
            assert_eq!(bin_to_id(&id), entry.log_id.index);
            self.db
                .put_cf(
                    &self.logs(),
                    id,
                    serde_json::to_vec(entry).map_err(|e| {
                        StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e))
                    })?,
                )
                .map_err(|e| {
                    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e))
                })?;
        }

        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<MapucheNodeId>,
    ) -> StorageResult<()> {
        let from = id_to_bin(log_id.index);
        let to = id_to_bin(0xff_ff_ff_ff_ff_ff_ff_ff);
        self.db
            .delete_range_cf(&self.logs(), &from, &to)
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e)).into()
            })
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<MapucheNodeId>,
    ) -> Result<(), StorageError<MapucheNodeId>> {
        self.set_last_purged(log_id)?;
        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index + 1);
        self.db
            .delete_range_cf(&self.logs(), &from, &to)
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e)).into()
            })
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
            state_machine.get_last_applied_log()?,
            state_machine.get_last_membership()?,
        ))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<TypeConfig>],
    ) -> Result<Vec<RaftResponse>, StorageError<MapucheNodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let sm = self.state_machine.write().await;

        for entry in entries {
            sm.set_last_applied_log(entry.log_id)?;

            match entry.payload {
                EntryPayload::Blank => res.push(RaftResponse::OptString(None)),
                EntryPayload::Normal(ref req) => match req {
                    RaftRequest::Set { key, value } => {
                        sm.insert(key.clone(), value.clone())?;
                        res.push(RaftResponse::OptString(Some(value.clone())))
                    }
                    RaftRequest::CmdLog { id, cmd } => {
                        let frame = cmd.clone().execute_for_remote().await.map_err(|e| {
                            StorageError::IO {
                                source: StorageIOError::new(
                                    ErrorSubject::StateMachine,
                                    ErrorVerb::Seek,
                                    AnyError::error(format!("raft request error: {:?}", e)),
                                ),
                            }
                        })?;
                        let cmd_str: String = cmd.into();
                        sm.insert(id.clone(), cmd_str.clone())?;
                        res.push(RaftResponse::Frame(frame))
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.set_last_membership(StoredMembership::new(Some(entry.log_id), mem.clone()))?;

                    res.push(RaftResponse::OptString(None))
                }
            };
        }

        self.flush(ErrorSubject::StateMachine, ErrorVerb::Write)?;
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
        let new_snapshot = RaftSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let updated_state_machine: SerializableRaftStateMachine =
                serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.signature()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine =
                RaftStateMachine::from_serializable(updated_state_machine, self.db.clone())?;
        }

        self.set_current_snapshot(new_snapshot)?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<Snapshot<MapucheNodeId, BasicNode, Self::SnapshotData>>,
        StorageError<MapucheNodeId>,
    > {
        match RaftStore::get_current_snapshot(self)? {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }
}
