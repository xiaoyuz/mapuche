use std::sync::Arc;
use rocksdb::{DB, WriteBatch};
use tokio::sync::Mutex;
use tokio::time::Instant;
use crate::rocks::errors::RError;
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
        let value: Vec<u8> = value.into();
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
                        let value: Value = opt.clone().unwrap_or_else(|| Vec::new()).into();
                        let kvpair = KvPair::new(key, value);
                        kvpairs.push(kvpair);
                    }
                    Err(_) => {}
                }
            }
            kvpairs
        }).await.unwrap();
        Ok(pairs)
    }
}