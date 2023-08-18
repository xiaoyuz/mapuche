use crate::rocks::client::RocksClient;

use crate::rocks::errors::RError;
use crate::rocks::kv::value::Value;
use crate::rocks::transaction::RocksTransaction;

use rocksdb::{MultiThreaded, Options, TransactionDB, TransactionDBOptions};

use std::{path::Path, sync::Arc};

pub mod client;
pub mod encoding;
pub mod errors;
pub mod hash;
pub mod kv;
pub mod list;
pub mod set;
pub mod string;
pub mod transaction;
pub mod zset;

pub const CF_NAME_GC: &str = "gc";
pub const CF_NAME_GC_VERSION: &str = "gc_version";
pub const CF_NAME_META: &str = "meta";
pub const CF_NAME_SET_SUB_META: &str = "set_sub_meta";
pub const CF_NAME_SET_DATA: &str = "set_data";
pub const CF_NAME_LIST_DATA: &str = "list_data";
pub const CF_NAME_HASH_SUB_META: &str = "hash_sub_meta";
pub const CF_NAME_HASH_DATA: &str = "hash_data";
pub const CF_NAME_ZSET_SUB_META: &str = "zset_sub_meta";
pub const CF_NAME_ZSET_DATA: &str = "zset_data";
pub const CF_NAME_ZSET_SCORE: &str = "zset_score";

pub type Result<T> = anyhow::Result<T, RError>;

pub static mut INSTANCE_ID: u64 = 0;

pub trait TxnCommand {
    fn txn_del(&self, txn: &RocksTransaction, key: &str) -> Result<()>;

    fn txn_expire_if_needed(&self, txn: &RocksTransaction, key: &str) -> Result<i64>;

    fn txn_expire(
        &self,
        txn: &RocksTransaction,
        key: &str,
        timestamp: i64,
        meta_value: &Value,
    ) -> Result<i64>;

    fn txn_gc(&self, txn: &RocksTransaction, key: &str, version: u16) -> Result<()>;
}

pub fn new_client<P: AsRef<Path>>(path: P) -> Result<RocksClient> {
    let db: TransactionDB = new_db(path)?;
    Ok(RocksClient::new(Arc::new(db)))
}

fn new_db<P: AsRef<Path>>(path: P) -> Result<TransactionDB<MultiThreaded>> {
    let mut opts = Options::default();
    let transaction_opts = TransactionDBOptions::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let cf_names = vec![
        CF_NAME_META,
        CF_NAME_GC,
        CF_NAME_GC_VERSION,
        CF_NAME_SET_SUB_META,
        CF_NAME_SET_DATA,
        CF_NAME_LIST_DATA,
        CF_NAME_HASH_SUB_META,
        CF_NAME_HASH_DATA,
        CF_NAME_ZSET_SUB_META,
        CF_NAME_ZSET_DATA,
        CF_NAME_ZSET_SCORE,
    ];

    TransactionDB::open_cf(&opts, &transaction_opts, path, cf_names).map_err(|e| e.into())
}

pub fn get_instance_id() -> u64 {
    unsafe { INSTANCE_ID }
}
