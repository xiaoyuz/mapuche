use std::sync::Arc;
use lazy_static::lazy_static;
use rocksdb::{Direction, IteratorMode, Transaction, TransactionDB};
use crate::rocks::client::RocksRawClient;
use crate::rocks::errors::{RError};
use crate::rocks::encoding::KeyEncoder;
use crate::rocks::kv::bound_range::BoundRange;
use crate::rocks::kv::key::Key;
use crate::rocks::kv::kvpair::KvPair;
use crate::rocks::kv::value::Value;

pub mod client;
pub mod errors;
pub mod string;
pub mod kv;
pub mod encoding;
pub mod set;

pub type Result<T> = std::result::Result<T, RError>;

pub static mut INSTANCE_ID: u64 = 0;

lazy_static! {
    pub static ref KEY_ENCODER: KeyEncoder = KeyEncoder::new();
    pub static ref ROCKS_DB: Arc<TransactionDB> = Arc::new(TransactionDB::open_default(".rocksdb_store").unwrap());
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

pub fn blocking_tx_scan(
    txn: &Transaction<TransactionDB>,
    range: impl Into<BoundRange>,
    limit: u32,
) -> Result<impl Iterator<Item=KvPair>> {
    let bound_range = range.into();
    let (start, end) = bound_range.into_keys();
    let start: Vec<u8> = start.into();
    let it = txn.iterator(
        IteratorMode::From(&start, Direction::Forward)
    );
    let end_it_key = end
        .and_then(|e| {
            let e_vec: Vec<u8> = e.into();
            Some(txn.iterator(
                IteratorMode::From(&e_vec, Direction::Forward)
            ))
        })
        .and_then(|mut it| it.next())
        .and_then(|res| res.ok())
        .and_then(|kv| Some(kv.0));

    let mut kv_pairs: Vec<KvPair> = Vec::new();
    for inner in it {
        if let Ok(kv_bytes) = inner {
            let pair: (Key, Value) = (kv_bytes.0.to_vec().into(), kv_bytes.1.to_vec());
            kv_pairs.push(pair.into());
            if &Some(kv_bytes.0) == &end_it_key {
                break;
            }
        }
        if kv_pairs.len() >= limit as usize {
            break;
        }
    }
    Ok(kv_pairs.into_iter())
}

#[cfg(test)]
mod tests {
    use rocksdb::{DB, Direction, IteratorMode, TransactionDB, WriteBatch, WriteBatchWithTransaction};
    use crate::rocks::kv::bound_range::BoundRange;
    use crate::rocks::kv::key::Key;
    use crate::rocks::blocking_tx_scan;

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
            println!("{:?}", inner_res);
        }
    }

    #[test]
    fn test_scan() {
        let db = TransactionDB::open_default(".rocksdb_store").unwrap();
        let mut batch = WriteBatchWithTransaction::default();
        batch.put(b"test000001", b"t1");
        batch.put(b"test000002", b"t2");
        batch.put(b"test000010", b"t3");
        batch.put(b"test001111", b"t4");
        batch.put(b"yyyyy", b"a1");
        db.write(batch).unwrap();

        let txn = db.transaction();
        let start_key: Key = Key::from("test000001".to_owned());
        let end_key: Key = Key::from("test001111".to_owned());
        let bound_range: BoundRange = (start_key..end_key).into();

        let it = blocking_tx_scan(&txn, bound_range, 2).unwrap();
        for inner in it {
            println!("{:?}", inner);
        }
    }
}