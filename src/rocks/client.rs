use std::sync::Arc;
use rocksdb::{DB, WriteBatch};
use crate::rocks::kv::key::Key;
use crate::rocks::kv::kvpair::KvPair;
use crate::rocks::kv::value::Value;
use crate::rocks::Result as RocksResult;

pub struct RocksRawClient {
    client: Arc<DB>,
}

impl RocksRawClient {
    pub fn new(client: Arc<DB>) -> Self {
        Self {
            client,
        }
    }

    pub async fn get(&self, key: Key) -> RocksResult<Option<Value>> {
        let client = self.client.clone();
        let key: Vec<u8> = key.into();
        tokio::spawn(async move {
            client.get(key)
        }).await.unwrap().map_err(|e| e.into())
    }

    pub async fn put(&self, key: Key, value: Value) -> RocksResult<()> {
        let client = self.client.clone();
        let key: Vec<u8> = key.into();
        let value: Vec<u8> = value;
        tokio::spawn(async move {
            client.put(key, value)
        }).await.unwrap().map_err(|e| e.into())
    }

    pub async fn del(&self, key: Key) -> RocksResult<()> {
        let client = self.client.clone();
        let key: Vec<u8> = key.into();
        tokio::spawn(async move {
            client.delete(key)
        }).await.unwrap().map_err(|e| e.into())
    }

    pub async fn batch_get(&self, keys: Vec<Key>) -> RocksResult<Vec<KvPair>> {
        let client = self.client.clone();
        let pairs = tokio::spawn(async move {
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
            kvpairs
        }).await.unwrap();
        Ok(pairs)
    }

    pub async fn batch_put(&self, kvs: Vec<KvPair>) -> RocksResult<()> {
        let client = self.client.clone();
        tokio::spawn(async move {
            let mut write_batch = WriteBatch::default();
            for kv in kvs {
                write_batch.put(kv.0, kv.1);
            }
            client.write(write_batch)
        }).await.unwrap().map_err(|e| e.into())
    }

    // TODO: Do it atomic
    pub async fn compare_and_swap(
        &self,
        key: Key,
        prev_val: Option<Value>,
        val: Value,
    ) -> RocksResult<(Option<Value>, bool)> {
        let client = self.client.clone();
        tokio::spawn(async move {
            let current = client.get(key.clone())?;
            if current == prev_val {
                client.put(key, val.clone())?;
                Ok((Some(val), true))
            } else {
                Ok((None, false))
            }
        }).await.unwrap()
    }
}