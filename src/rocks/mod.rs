use std::sync::Arc;
use lazy_static::lazy_static;
use rocksdb::{MultiThreaded, Options, TransactionDB, TransactionDBOptions};
use crate::config::config_meta_key_number_or_default;
use crate::fetch_idx_and_add;
use crate::rocks::client::RocksRawClient;
use crate::rocks::errors::RError;
use crate::rocks::encoding::KeyEncoder;
use crate::rocks::transaction::RocksTransaction;

pub mod client;
pub mod errors;
pub mod string;
pub mod kv;
pub mod encoding;
pub mod set;
pub mod transaction;

pub const CF_NAME_GC: &str = "gc";
pub const CF_NAME_META: &str = "meta";
pub const CF_NAME_SET_SUB_META: &str = "set_sub_meta";
pub const CF_NAME_SET_DATA: &str = "set_data";

pub type Result<T> = std::result::Result<T, RError>;

pub static mut INSTANCE_ID: u64 = 0;

lazy_static! {
    pub static ref KEY_ENCODER: KeyEncoder = KeyEncoder::new();
    pub static ref ROCKS_DB: Arc<TransactionDB> = Arc::new(new_db().unwrap());
}

pub trait RocksCommand {
    fn txn_del(
        &self,
        txn: &RocksTransaction,
        client: &RocksRawClient,
        key: &str,
    ) -> Result<()>;

    fn txn_expire_if_needed(
        self,
        txn: &RocksTransaction,
        client: &RocksRawClient,
        key: &str
    ) -> Result<i64>;
}

fn new_db() -> Result<TransactionDB<MultiThreaded>> {
    let mut opts = Options::default();
    let transaction_opts = TransactionDBOptions::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let cf_names = vec![
        CF_NAME_META,
        CF_NAME_GC,
        CF_NAME_SET_SUB_META, CF_NAME_SET_DATA,
    ];

    TransactionDB::open_cf(
        &opts, &transaction_opts, ".rocksdb_store", cf_names
    ).map_err(|e| e.into())
}

pub fn set_instance_id(id: u64) {
    unsafe {
        INSTANCE_ID = id;
    }
}

pub fn get_instance_id() -> u64 {
    unsafe { INSTANCE_ID }
}

pub fn get_client() -> RocksRawClient {
    let db = ROCKS_DB.clone();
    RocksRawClient::new(db)
}

pub fn gen_next_meta_index() -> u16 {
    fetch_idx_and_add() % config_meta_key_number_or_default()
}

#[cfg(test)]
mod tests {
    use rocksdb::{Direction, IteratorMode, TransactionDB, WriteBatchWithTransaction};

    #[test]
    fn test_rocksdb() {
        let db: TransactionDB = TransactionDB::open_default(".rocksdb_store").unwrap();
        db.put(b"my key", b"my value").unwrap();
        match db.get(b"my key") {
            Ok(Some(value)) => println!("retrieved value {}", String::from_utf8(value).unwrap()),
            Ok(None) => println!("value not found"),
            Err(e) => println!("operational problem encountered: {e}"),
        }
        db.delete(b"my key").unwrap();

        let mut batch = WriteBatchWithTransaction::default();
        batch.put(b"test000001", b"t1");
        batch.put(b"test000002", b"t2");
        batch.put(b"test000010", b"t3");
        batch.put(b"test001111", b"t3");
        batch.put(b"yyyyy", b"a1");
        db.write(batch).unwrap();

        let txn = db.transaction();

        let it = txn.iterator(IteratorMode::From(b"test000003", Direction::Forward));
        let mut it_stop = txn.iterator(IteratorMode::From(b"yyyyy", Direction::Reverse));
        let stop = it_stop.next().unwrap().unwrap();
        for inner in it {
            let inner_res = inner.unwrap();
            if inner_res == stop {
                break;
            }
            println!("{inner_res:?}");
        }
    }
}