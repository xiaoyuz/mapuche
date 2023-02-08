use std::sync::Arc;
use rocksdb::{Transaction, TransactionDB, WriteBatchWithTransaction};

use crate::rocks::errors::TXN_ERROR;
use crate::rocks::kv::key::Key;
use crate::rocks::kv::kvpair::KvPair;
use crate::rocks::kv::value::Value;
use crate::rocks::Result as RocksResult;

pub struct RocksRawClient {
    client: Arc<TransactionDB>,
}

impl RocksRawClient {
    pub fn new(client: Arc<TransactionDB>) -> Self {
        Self {
            client,
        }
    }

    pub fn get(&self, key: Key) -> RocksResult<Option<Value>> {
        let client = self.client.clone();
        let key: Vec<u8> = key.into();
        client.get(key).map_err(|e| e.into())
    }

    pub fn put(&self, key: Key, value: Value) -> RocksResult<()> {
        let client = self.client.clone();
        let key: Vec<u8> = key.into();
        let value: Vec<u8> = value;
        client.put(key, value).map_err(|e| e.into())
    }

    pub fn del(&self, key: Key) -> RocksResult<()> {
        let client = self.client.clone();
        let key: Vec<u8> = key.into();
        client.delete(key).map_err(|e| e.into())
    }

    pub fn batch_get(&self, keys: Vec<Key>) -> RocksResult<Vec<KvPair>> {
        let client = self.client.clone();
        let results = client.multi_get(&keys);
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

    pub fn batch_put(&self, kvs: Vec<KvPair>) -> RocksResult<()> {
        let client = self.client.clone();
        let mut write_batch = WriteBatchWithTransaction::default();
        for kv in kvs {
            write_batch.put(kv.0, kv.1);
        }
        client.write(write_batch).map_err(|e| e.into())
    }

    pub fn exec_txn<T, F>(
        &self,
        f: F,
    ) -> RocksResult<T>
    where
        T: Send + Sync + 'static,
        F: FnOnce(&Transaction<TransactionDB>) -> RocksResult<T> + Send + 'static {
        let client = self.client.clone();
        let txn = client.transaction();
        let res = f(&txn)?;
        if txn.commit().is_err() {
            return Err(TXN_ERROR);
        }
        Ok(res)
    }
}