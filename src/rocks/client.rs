use crate::config::async_deletion_enabled_or_default;
use rocksdb::{
    ColumnFamilyRef, TransactionDB, TransactionOptions, WriteBatchWithTransaction, WriteOptions,
};
use std::sync::Arc;

use crate::rocks::errors::{CF_NOT_EXISTS_ERR, KEY_VERSION_EXHUSTED_ERR, TXN_ERROR};
use crate::rocks::kv::bound_range::BoundRange;
use crate::rocks::kv::key::Key;
use crate::rocks::kv::kvpair::KvPair;
use crate::rocks::kv::value::Value;
use crate::rocks::transaction::RocksTransaction;
use crate::rocks::Result as RocksResult;

use super::encoding::KeyEncoder;

pub struct RocksClient {
    client: Arc<TransactionDB>,
}

impl RocksClient {
    pub fn new(client: Arc<TransactionDB>) -> Self {
        Self { client }
    }

    pub fn get(&self, cf: ColumnFamilyRef, key: Key) -> RocksResult<Option<Value>> {
        let client = self.client.as_ref();
        let key: Vec<u8> = key.into();
        client.get_cf(&cf, key).map_err(|e| e.into())
    }

    pub fn put(&self, cf: ColumnFamilyRef, key: Key, value: Value) -> RocksResult<()> {
        let client = self.client.as_ref();
        let key: Vec<u8> = key.into();
        let value: Vec<u8> = value;
        client.put_cf(&cf, key, value).map_err(|e| e.into())
    }

    pub fn del(&self, cf: ColumnFamilyRef, key: Key) -> RocksResult<()> {
        let client = self.client.as_ref();
        let key: Vec<u8> = key.into();
        client.delete_cf(&cf, key).map_err(|e| e.into())
    }

    pub fn batch_get(&self, cf: ColumnFamilyRef, keys: Vec<Key>) -> RocksResult<Vec<KvPair>> {
        let client = self.client.as_ref();

        let cf_key_pairs = keys
            .clone()
            .into_iter()
            .map(|k| (&cf, k))
            .collect::<Vec<(&ColumnFamilyRef, Key)>>();

        let results = client.multi_get_cf(cf_key_pairs);
        let mut kvpairs = Vec::new();
        for i in 0..results.len() {
            if let Ok(opt) = results.get(i).unwrap() {
                let key = keys.get(i).unwrap().clone();
                let value = opt.clone();
                if let Some(val) = value {
                    let kvpair = KvPair::from((key, val));
                    kvpairs.push(kvpair);
                }
            }
        }
        Ok(kvpairs)
    }

    pub fn batch_put(&self, cf: ColumnFamilyRef, kvs: Vec<KvPair>) -> RocksResult<()> {
        let client = self.client.as_ref();

        let mut write_batch = WriteBatchWithTransaction::default();
        for kv in kvs {
            write_batch.put_cf(&cf, kv.0, kv.1);
        }
        client.write(write_batch).map_err(|e| e.into())
    }

    pub fn cf_handle(&self, name: &str) -> RocksResult<ColumnFamilyRef> {
        self.client.cf_handle(name).ok_or(CF_NOT_EXISTS_ERR)
    }

    pub fn scan(
        &self,
        cf_handle: ColumnFamilyRef,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> RocksResult<impl Iterator<Item = KvPair>> {
        let bound_range = range.into();
        let (start, end) = bound_range.into_keys();
        let start: Vec<u8> = start.into();
        let it = self.client.prefix_iterator_cf(&cf_handle, &start);
        let end_it_key = end
            .map(|e| {
                let e_vec: Vec<u8> = e.into();
                self.client.prefix_iterator_cf(&cf_handle, e_vec)
            })
            .and_then(|mut it| it.next())
            .and_then(|res| res.ok())
            .map(|kv| kv.0);

        let mut kv_pairs: Vec<KvPair> = Vec::new();
        for inner in it {
            if let Ok(kv_bytes) = inner {
                if Some(&kv_bytes.0) == end_it_key.as_ref() {
                    break;
                }
                let pair: (Key, Value) = (kv_bytes.0.to_vec().into(), kv_bytes.1.to_vec());
                kv_pairs.push(pair.into());
            }
            if kv_pairs.len() >= limit as usize {
                break;
            }
        }
        Ok(kv_pairs.into_iter())
    }

    pub fn exec_txn<T, F>(&self, f: F) -> RocksResult<T>
    where
        T: Send + Sync + 'static,
        F: FnOnce(&RocksTransaction) -> RocksResult<T>,
    {
        let client = self.client.as_ref();
        let txn_opts = TransactionOptions::new();
        // txn_opts.set_lock_timeout(50);
        let txn = client.transaction_opt(&WriteOptions::default(), &txn_opts);
        let rock_txn = RocksTransaction::new(txn);
        let res = f(&rock_txn)?;
        if rock_txn.commit().is_err() {
            return Err(TXN_ERROR);
        }
        Ok(res)
    }
}

// get_version_for_new must be called outside of a MutexGuard, otherwise it will deadlock.
pub fn get_version_for_new(
    txn: &RocksTransaction,
    gc_cf: ColumnFamilyRef,
    gc_version_cf: ColumnFamilyRef,
    key: &str,
    key_encoder: &KeyEncoder,
) -> RocksResult<u16> {
    // check if async deletion is enabled, return ASAP if not
    if !async_deletion_enabled_or_default() {
        return Ok(0);
    }

    let gc_key = key_encoder.encode_gc_key(key);
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
    let gc_version_key = key_encoder.encode_gc_version_key(key, next_version);
    txn.get(gc_version_cf, gc_version_key)?
        .map_or_else(|| Ok(next_version), |_| Err(KEY_VERSION_EXHUSTED_ERR))
}
