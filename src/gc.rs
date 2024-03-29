use crate::config::{
    async_deletion_enabled_or_default, async_gc_interval_or_default,
    async_gc_worker_queue_size_or_default, LOGGER,
};
use crate::metrics::GC_TASK_QUEUE_COUNTER;
use crate::rocks::client::RocksClient;
use crate::rocks::encoding::{DataType, KeyDecoder};
use crate::rocks::errors::RError;
use crate::rocks::hash::HashCommand;
use crate::rocks::list::ListCommand;
use crate::rocks::{get_client, TxnCommand, CF_NAME_GC, CF_NAME_GC_VERSION, KEY_ENCODER};
use crc::{Crc, CRC_16_XMODEM};
use rocksdb::ColumnFamilyRef;
use slog::{debug, error, info};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex};
use tokio::time;
use tokio::time::MissedTickBehavior;

use crate::rocks::set::SetCommand;
use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;

const CRC16: Crc<u16> = Crc::<u16>::new(&CRC_16_XMODEM);

pub struct GcCF<'a> {
    gc_cf: ColumnFamilyRef<'a>,
    gc_version_cf: ColumnFamilyRef<'a>,
}

impl<'a> GcCF<'a> {
    pub fn new(client: &'a RocksClient) -> Self {
        GcCF {
            gc_cf: client.cf_handle(CF_NAME_GC).unwrap(),
            gc_version_cf: client.cf_handle(CF_NAME_GC_VERSION).unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GcTask {
    key_type: DataType,
    user_key: Vec<u8>,
    version: u16,
}

impl GcTask {
    fn new(key_type: DataType, user_key: Vec<u8>, version: u16) -> GcTask {
        GcTask {
            key_type,
            user_key,
            version,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(3 + self.user_key.len());
        bytes.push(self.key_type.clone() as u8);
        bytes.extend_from_slice(&self.user_key);
        bytes.extend_from_slice(&self.version.to_be_bytes());
        bytes
    }
}

#[derive(Debug, Clone)]
pub struct GcMaster {
    workers: Vec<GcWorker>,
}

impl GcMaster {
    pub fn new(worker_num: usize) -> Self {
        let mut workers = Vec::with_capacity(worker_num);

        // create workers pool
        for id in 0..worker_num {
            let (tx, rx) = mpsc::channel::<GcTask>(async_gc_worker_queue_size_or_default());
            let worker = GcWorker::new(id, rx, tx);
            workers.push(worker);
        }

        GcMaster { workers }
    }

    pub async fn start_workers(&self) {
        // run all workers
        // worker will wait for task from channel
        for worker in &self.workers {
            worker.clone().run().await;
        }
    }

    // dispatch task to a worker
    pub async fn dispatch_task(&mut self, task: GcTask) -> RocksResult<()> {
        // calculate task hash and dispatch to worker
        let idx = CRC16.checksum(&task.to_bytes()) as usize % self.workers.len();
        debug!(LOGGER, "[GC] dispatch task {:?} to worker: {}", task, idx);
        self.workers[idx].add_task(task).await
    }

    // scan gc version keys
    // create gc task for each version key
    // dispatch gc task to workers
    pub async fn run(&mut self) -> RocksResult<()> {
        let mut interval = time::interval(Duration::from_millis(async_gc_interval_or_default()));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let client = get_client();
        let gc_cfs = GcCF::new(&client);
        loop {
            interval.tick().await;
            if !async_deletion_enabled_or_default() {
                continue;
            }

            let bound_range = KEY_ENCODER.encode_gc_version_key_range();

            // TODO scan speed throttling
            let iter_res = client.scan(gc_cfs.gc_version_cf.clone(), bound_range, u32::MAX);
            if iter_res.is_err() {
                error!(
                    LOGGER,
                    "[GC] scan gc version keys failed: {:?}",
                    iter_res.err()
                );
                // retry next tick
                continue;
            }

            let iter = iter_res.unwrap();
            for kv in iter {
                let (user_key, version) = KeyDecoder::decode_key_gc_userkey_version(kv.0);
                let key_type = match kv.1[0] {
                    0 => DataType::String,
                    1 => DataType::Hash,
                    2 => DataType::List,
                    3 => DataType::Set,
                    4 => DataType::Zset,
                    _ => DataType::Null,
                };
                let task = GcTask::new(key_type, user_key, version);
                if let Err(e) = self.dispatch_task(task).await {
                    error!(LOGGER, "[GC] dispatch task failed: {:?}", e);
                }
            }
        }
    }

    pub fn shutdown(&self) {}
}

#[derive(Debug, Clone)]
struct GcWorker {
    id: usize,

    rx: Arc<Mutex<Receiver<GcTask>>>,
    tx: Sender<GcTask>,

    // check task already in queue, avoid duplicate task
    task_sets: Arc<Mutex<HashSet<Vec<u8>>>>,
}

impl GcWorker {
    pub fn new(id: usize, rx: Receiver<GcTask>, tx: Sender<GcTask>) -> Self {
        GcWorker {
            id,
            rx: Arc::new(Mutex::new(rx)),
            tx,
            task_sets: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    // queue task to channel
    pub async fn add_task(&mut self, task: GcTask) -> RocksResult<()> {
        let bytes = task.to_bytes();
        let mut task_sets = self.task_sets.lock().await;
        if !task_sets.contains(&bytes) {
            debug!(LOGGER, "[GC] add task: {:?}", task);
            task_sets.insert(bytes);
            return if let Err(e) = self.tx.send(task).await {
                error!(LOGGER, "[GC] send task to channel failed"; "error" => ?e);
                Err(RError::Owned(e.to_string()))
            } else {
                GC_TASK_QUEUE_COUNTER
                    .with_label_values(&[&self.id.to_string()])
                    .inc();
                Ok(())
            };
        }
        Ok(())
    }

    pub async fn handle_task(&self, task: GcTask) -> RocksResult<()> {
        let client = get_client();
        let gc_cfs = GcCF::new(&client);
        client.exec_txn(|txn| {
            let task = task.clone();
            let user_key = String::from_utf8_lossy(&task.user_key);
            let version = task.version;
            match task.key_type {
                DataType::String => {
                    panic!("string not support async deletion");
                }
                DataType::Set => {
                    debug!(
                        LOGGER,
                        "[GC] async delete set key {} with version {}", user_key, version
                    );
                    SetCommand::new(&client).txn_gc(txn, &client, &user_key, version)?;
                }
                DataType::List => {
                    debug!(
                        LOGGER,
                        "[GC] async delete list key {} with version {}", user_key, version
                    );
                    ListCommand::new(&client).txn_gc(txn, &client, &user_key, version)?;
                }
                DataType::Hash => {
                    debug!(
                        LOGGER,
                        "[GC] async delete hash key {} with version {}", user_key, version
                    );
                    HashCommand::new(&client).txn_gc(txn, &client, &user_key, version)?;
                }
                DataType::Zset => {
                    debug!(
                        LOGGER,
                        "[GC] async delete zset key {} with version {}", user_key, version
                    );
                    ZsetCommand::new(&client).txn_gc(txn, &client, &user_key, version)?;
                }
                DataType::Null => {
                    panic!("unknown data type to do async deletion");
                }
            }
            // delete gc version key
            let gc_version_key = KEY_ENCODER.encode_gc_version_key(&user_key, version);
            txn.del(gc_cfs.gc_version_cf.clone(), gc_version_key)?;
            Ok(())
        })?;

        // check the gc key in a small txn, avoid transaction confliction
        client.exec_txn(|txn| {
            let task = task.clone();
            let user_key = String::from_utf8_lossy(&task.user_key);
            // also delete gc key if version in gc key is same as task.version
            let gc_key = KEY_ENCODER.encode_gc_key(&user_key);
            let version = task.version;
            if let Some(v) = txn.get(gc_cfs.gc_cf.clone(), gc_key.clone())? {
                let ver = u16::from_be_bytes(v[..2].try_into().unwrap());
                if ver == version {
                    debug!(
                        LOGGER,
                        "[GC] clean gc key for user key {} with version {}", user_key, version
                    );
                    txn.del(gc_cfs.gc_cf.clone(), gc_key)?;
                }
            }
            Ok(())
        })
    }

    pub async fn run(self) {
        tokio::spawn(async move {
            info!(LOGGER, "[GC] start gc worker thread: {}", self.id);
            while let Some(task) = self.rx.lock().await.recv().await {
                match self.handle_task(task.clone()).await {
                    Ok(_) => {
                        debug!(LOGGER, "[GC] gc task done: {:?}", task);
                        self.task_sets.lock().await.remove(&task.to_bytes());
                        GC_TASK_QUEUE_COUNTER
                            .with_label_values(&[&self.id.to_string()])
                            .dec();
                    }
                    Err(e) => {
                        error!(LOGGER, "[GC] handle task error: {:?}", e);
                    }
                }
            }
            info!(LOGGER, "[GC] gc worker thread exit: {}", self.id);
        });
    }
}
