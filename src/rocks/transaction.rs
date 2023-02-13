
use rocksdb::{ColumnFamilyRef, Transaction, TransactionDB};
use crate::metrics::ROCKS_ERR_COUNTER;


use crate::rocks::errors::{TXN_ERROR};
use crate::rocks::kv::key::Key;
use crate::rocks::kv::kvpair::KvPair;
use crate::rocks::kv::value::Value;
use crate::rocks::{Result as RocksResult};
use crate::rocks::kv::bound_range::BoundRange;

pub struct RocksTransaction<'a> {
    inner_txn: Transaction<'a, TransactionDB>,
}

impl<'a> RocksTransaction<'a> {
    pub fn new(txn: Transaction<'a, TransactionDB>) -> Self {
        Self {
            inner_txn: txn,
        }
    }

    pub fn get(&self, cf: ColumnFamilyRef, key: Key) -> RocksResult<Option<Value>> {
        let key: Vec<u8> = key.into();
        self.inner_txn.get_cf(&cf, key).map_err(|e| {
            ROCKS_ERR_COUNTER
                .with_label_values(&["txn_client_error"])
                .inc();
            e.into()
        })
    }

    pub fn put(&self, cf: ColumnFamilyRef, key: Key, value: Value) -> RocksResult<()> {
        let key: Vec<u8> = key.into();
        let value: Vec<u8> = value;
        self.inner_txn.put_cf(&cf, key, value).map_err(|e| {
            ROCKS_ERR_COUNTER
                .with_label_values(&["txn_client_error"])
                .inc();
            e.into()
        })
    }

    pub fn del(&self, cf: ColumnFamilyRef, key: Key) -> RocksResult<()> {
        let key: Vec<u8> = key.into();
        self.inner_txn.delete_cf(&cf, key).map_err(|e| {
            ROCKS_ERR_COUNTER
                .with_label_values(&["txn_client_error"])
                .inc();
            e.into()
        })
    }

    pub fn batch_get(&self, cf: ColumnFamilyRef, keys: Vec<Key>) -> RocksResult<Vec<KvPair>> {
        let cf_key_pairs = keys.clone().into_iter().map(|k| (&cf, k))
            .collect::<Vec<(&ColumnFamilyRef, Key)>>();

        let results = self.inner_txn.multi_get_cf(cf_key_pairs);
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
                        .with_label_values(&["txn_client_error"])
                        .inc();
                }
            }
        }
        Ok(kvpairs)
    }

    pub fn commit(self) -> RocksResult<()> {
        self.inner_txn.commit().map_err(|_| {
            ROCKS_ERR_COUNTER
                .with_label_values(&["txn_client_error"])
                .inc();
            TXN_ERROR
        })
    }

    pub fn scan(
        &self,
        cf_handle: ColumnFamilyRef,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> RocksResult<impl Iterator<Item=KvPair>> {
        let bound_range = range.into();
        let (start, end) = bound_range.into_keys();
        let start: Vec<u8> = start.into();
        let it = self.inner_txn.prefix_iterator_cf(&cf_handle, &start);
        let end_it_key = end
            .map(|e| {
                let e_vec: Vec<u8> = e.into();
                self.inner_txn.prefix_iterator_cf(&cf_handle, e_vec)
            })
            .and_then(|mut it| it.next())
            .and_then(|res| res.ok()).map(|kv| kv.0);

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

    pub fn scan_keys(
        &self,
        cf_handle: ColumnFamilyRef,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> RocksResult<impl Iterator<Item=Key>> {
        let bound_range = range.into();
        let (start, end) = bound_range.into_keys();
        let start: Vec<u8> = start.into();
        let it = self.inner_txn.prefix_iterator_cf(&cf_handle, &start);
        let end_it_key = end
            .map(|e| {
                let e_vec: Vec<u8> = e.into();
                self.inner_txn.prefix_iterator_cf(&cf_handle, e_vec)
            })
            .and_then(|mut it| it.next())
            .and_then(|res| res.ok()).map(|kv| kv.0);

        let mut keys: Vec<Key> = Vec::new();
        for inner in it {
            if let Ok(kv_bytes) = inner {
                if Some(&kv_bytes.0) == end_it_key.as_ref() {
                    break;
                }
                keys.push(kv_bytes.0.to_vec().into());
            }
            if keys.len() >= limit as usize {
                break;
            }
        }
        Ok(keys.into_iter())
    }
}