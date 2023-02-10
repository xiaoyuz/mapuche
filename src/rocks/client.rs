use std::sync::Arc;
use rocksdb::{AsColumnFamilyRef, BoundColumnFamily, Transaction, TransactionDB, WriteBatchWithTransaction};
use crate::config::async_deletion_enabled_or_default;

use crate::rocks::errors::{CF_NOT_EXISTS_ERR, KEY_VERSION_EXHUSTED_ERR, TXN_ERROR};
use crate::rocks::kv::key::Key;
use crate::rocks::kv::kvpair::KvPair;
use crate::rocks::kv::value::Value;
use crate::rocks::{KEY_ENCODER, Result as RocksResult};

pub struct RocksRawClient {
    client: Arc<TransactionDB>,
}

impl RocksRawClient {
    pub fn new(client: Arc<TransactionDB>) -> Self {
        Self {
            client,
        }
    }

    pub fn get(&self, cf: &str, key: Key) -> RocksResult<Option<Value>> {
        let client = self.client.clone();
        let cf = self.cf_handle(cf)?;
        let key: Vec<u8> = key.into();
        client.get_cf(&cf, key).map_err(|e| e.into())
    }

    pub fn put(&self, cf: &str, key: Key, value: Value) -> RocksResult<()> {
        let client = self.client.clone();
        let cf = self.cf_handle(cf)?;
        let key: Vec<u8> = key.into();
        let value: Vec<u8> = value;
        client.put_cf(&cf, key, value).map_err(|e| e.into())
    }

    pub fn del(&self, cf: &str, key: Key) -> RocksResult<()> {
        let client = self.client.clone();
        let cf = self.cf_handle(cf)?;
        let key: Vec<u8> = key.into();
        client.delete_cf(&cf, key).map_err(|e| e.into())
    }

    pub fn batch_get(&self, cf: &str, keys: Vec<Key>) -> RocksResult<Vec<KvPair>> {
        let client = self.client.clone();
        let cf = self.cf_handle(cf)?;

        let cf_key_pairs = keys.clone().into_iter().map(|k| (&cf, k))
            .collect::<Vec<(&Arc<BoundColumnFamily>, Key)>>();

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
                Err(_) => {}
            }
        }
        Ok(kvpairs)
    }

    pub fn batch_put(&self, cf: &str, kvs: Vec<KvPair>) -> RocksResult<()> {
        let client = self.client.clone();
        let cf = self.cf_handle(cf)?;

        let mut write_batch = WriteBatchWithTransaction::default();
        for kv in kvs {
            write_batch.put_cf(&cf, kv.0, kv.1);
        }
        client.write(write_batch).map_err(|e| e.into())
    }

    pub fn cf_handle(&self, name: &str) -> RocksResult<Arc<BoundColumnFamily>> {
        self.client.cf_handle(name).ok_or(CF_NOT_EXISTS_ERR)
    }

    pub fn exec_txn<T, F>(
        &self,
        f: F,
    ) -> RocksResult<T>
    where
        T: Send + Sync + 'static,
        F: FnOnce(&Transaction<TransactionDB>) -> RocksResult<T> {
        let client = self.client.clone();
        let txn = client.transaction();
        let res = f(&txn)?;
        if txn.commit().is_err() {
            return Err(TXN_ERROR);
        }
        Ok(res)
    }
}

// get_version_for_new must be called outside of a MutexGuard, otherwise it will deadlock.
pub fn get_version_for_new(
    txn: &Transaction<TransactionDB>,
    gc_cf: &impl AsColumnFamilyRef,
    key: &str
) -> RocksResult<u16> {
    // check if async deletion is enabled, return ASAP if not
    if !async_deletion_enabled_or_default() {
        return Ok(0);
    }

    let gc_key = KEY_ENCODER.encode_txn_kv_gc_key(key);
    let next_version = txn.get_cf(gc_cf, gc_key)?.map_or_else(
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
    txn.get_cf(gc_cf, gc_version_key)?
        .map_or_else(|| Ok(next_version), |_| Err(KEY_VERSION_EXHUSTED_ERR))
}