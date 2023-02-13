use std::sync::Arc;
use rocksdb::{ColumnFamilyRef, TransactionDB, WriteBatchWithTransaction};
use tokio::time::Instant;
use crate::config::async_deletion_enabled_or_default;
use crate::metrics::{ROCKS_ERR_COUNTER, TXN_COUNTER, TXN_DURATION};

use crate::rocks::errors::{CF_NOT_EXISTS_ERR, KEY_VERSION_EXHUSTED_ERR, TXN_ERROR};
use crate::rocks::kv::key::Key;
use crate::rocks::kv::kvpair::KvPair;
use crate::rocks::kv::value::Value;
use crate::rocks::{KEY_ENCODER, Result as RocksResult};
use crate::rocks::transaction::RocksTransaction;
use crate::server::duration_to_sec;

pub struct RocksRawClient {
    client: Arc<TransactionDB>,
}

impl RocksRawClient {
    pub fn new(client: Arc<TransactionDB>) -> Self {
        Self {
            client,
        }
    }

    pub fn get(&self, cf: ColumnFamilyRef, key: Key) -> RocksResult<Option<Value>> {
        let client = self.client.clone();
        let key: Vec<u8> = key.into();
        client.get_cf(&cf, key).map_err(|e| {
            ROCKS_ERR_COUNTER
                .with_label_values(&["raw_client_error"])
                .inc();
            e.into()
        })
    }

    pub fn put(&self, cf: ColumnFamilyRef, key: Key, value: Value) -> RocksResult<()> {
        let client = self.client.clone();
        let key: Vec<u8> = key.into();
        let value: Vec<u8> = value;
        client.put_cf(&cf, key, value).map_err(|e| {
            ROCKS_ERR_COUNTER
                .with_label_values(&["raw_client_error"])
                .inc();
            e.into()
        })
    }

    pub fn del(&self, cf: ColumnFamilyRef, key: Key) -> RocksResult<()> {
        let client = self.client.clone();
        let key: Vec<u8> = key.into();
        client.delete_cf(&cf, key).map_err(|e| {
            ROCKS_ERR_COUNTER
                .with_label_values(&["raw_client_error"])
                .inc();
            e.into()
        })
    }

    pub fn batch_get(&self, cf: ColumnFamilyRef, keys: Vec<Key>) -> RocksResult<Vec<KvPair>> {
        let client = self.client.clone();

        let cf_key_pairs = keys.clone().into_iter().map(|k| (&cf, k))
            .collect::<Vec<(&ColumnFamilyRef, Key)>>();

        let results = client.multi_get_cf(cf_key_pairs);
        let mut kvpairs = Vec::new();
        for i in 0..results.len() {
            match results.get(i).unwrap() {
                Ok(opt) => {
                    let key = keys.get(i).unwrap().clone();
                    let value = opt.clone();
                    if let Some(val) = value {
                        let kvpair = KvPair::from((key, val));
                        kvpairs.push(kvpair);
                    }
                }
                Err(_) => {
                    ROCKS_ERR_COUNTER
                        .with_label_values(&["raw_client_error"])
                        .inc();
                }
            }
        }
        Ok(kvpairs)
    }

    pub fn batch_put(&self, cf: ColumnFamilyRef, kvs: Vec<KvPair>) -> RocksResult<()> {
        let client = self.client.clone();

        let mut write_batch = WriteBatchWithTransaction::default();
        for kv in kvs {
            write_batch.put_cf(&cf, kv.0, kv.1);
        }
        client.write(write_batch).map_err(|e| {
            ROCKS_ERR_COUNTER
                .with_label_values(&["raw_client_error"])
                .inc();
            e.into()
        })
    }

    pub fn cf_handle(&self, name: &str) -> RocksResult<ColumnFamilyRef> {
        self.client.cf_handle(name).ok_or_else(|| CF_NOT_EXISTS_ERR)
    }

    pub fn exec_txn<T, F>(
        &self,
        f: F,
    ) -> RocksResult<T>
    where
        T: Send + Sync + 'static,
        F: FnOnce(&RocksTransaction) -> RocksResult<T> {
        let client = self.client.clone();
        let txn = client.transaction();
        TXN_COUNTER.inc();
        let rock_txn = RocksTransaction::new(txn);
        let start_at = Instant::now();
        let res = f(&rock_txn)?;
        if rock_txn.commit().is_err() {
            ROCKS_ERR_COUNTER
                .with_label_values(&["txn_client_error"])
                .inc();
            return Err(TXN_ERROR);
        }
        let duration = Instant::now() - start_at;
        TXN_DURATION.observe(duration_to_sec(duration));
        Ok(res)
    }
}

// get_version_for_new must be called outside of a MutexGuard, otherwise it will deadlock.
pub fn get_version_for_new(
    txn: &RocksTransaction,
    gc_cf: ColumnFamilyRef,
    key: &str
) -> RocksResult<u16> {
    // check if async deletion is enabled, return ASAP if not
    if !async_deletion_enabled_or_default() {
        return Ok(0);
    }

    let gc_key = KEY_ENCODER.encode_txn_kv_gc_key(key);
    let next_version = txn.get(gc_cf.clone(), gc_key)?.map_or_else(
        || 0,
        |v| {
            let version = u16::from_be_bytes(v[..].try_into().unwrap());
            if version == u16::MAX {
                0
            } else {
                version + 1
            }
        },
    );
    // check next version available
    let gc_version_key = KEY_ENCODER.encode_txn_kv_gc_version_key(key, next_version);
    txn.get(gc_cf, gc_version_key)?
        .map_or_else(|| Ok(next_version), |_| Err(KEY_VERSION_EXHUSTED_ERR))
}