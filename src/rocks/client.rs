use std::sync::Arc;
use rocksdb::DB;
use crate::rocks::kv::key::Key;
use crate::rocks::kv::value::Value;
use crate::rocks::Result as RocksResult;

pub struct RocksClientWrapper {
    client: Arc<DB>,
}

impl RocksClientWrapper {
    pub fn new() -> RocksResult<Self> {
        let db = DB::open_default(".rocksdb_store")?;
        Ok(Self {
            client: Arc::new(db),
        })
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
}