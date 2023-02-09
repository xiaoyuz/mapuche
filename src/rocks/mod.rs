use std::collections::HashMap;
use std::sync::Arc;
use lazy_static::lazy_static;
use rocksdb::{AsColumnFamilyRef, BoundColumnFamily, Direction, IteratorMode, MultiThreaded, Options, Transaction, TransactionDB, TransactionDBOptions};
use crate::config::config_meta_key_number_or_default;
use crate::fetch_idx_and_add;
use crate::rocks::client::RocksRawClient;
use crate::rocks::errors::RError;
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

pub const CF_NAME_STRING_DATA: &str = "string_data";
pub const CF_NAME_SET_META: &str = "set_meta";
pub const CF_NAME_SET_SUB_META: &str = "set_sub_meta";
pub const CF_NAME_SET_GC: &str = "set_gc";
pub const CF_NAME_SET_DATA: &str = "set_data";

pub type Result<T> = std::result::Result<T, RError>;

pub static mut INSTANCE_ID: u64 = 0;

lazy_static! {
    pub static ref KEY_ENCODER: KeyEncoder = KeyEncoder::new();
    pub static ref ROCKS_DB: Arc<TransactionDB> = Arc::new(new_db().unwrap());
}

fn new_db() -> Result<TransactionDB<MultiThreaded>> {
    let mut opts = Options::default();
    let mut transaction_opts = TransactionDBOptions::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let cf_names = vec![
        CF_NAME_STRING_DATA,
        CF_NAME_SET_META, CF_NAME_SET_SUB_META, CF_NAME_SET_GC, CF_NAME_SET_DATA,
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

pub fn tx_scan(
    txn: &Transaction<TransactionDB>,
    range: impl Into<BoundRange>,
    limit: u32,
) -> Result<impl Iterator<Item=KvPair>> {
    let bound_range = range.into();
    let (start, end) = bound_range.into_keys();
    let start: Vec<u8> = start.into();
    let it = txn.prefix_iterator(&start);
    let end_it_key = end
        .map(|e| {
            let e_vec: Vec<u8> = e.into();
            txn.prefix_iterator(&e_vec)
        })
        .and_then(|mut it| it.next())
        .and_then(|res| res.ok()).map(|kv| kv.0);

    let mut kv_pairs: Vec<KvPair> = Vec::new();
    for inner in it {
        if let Ok(kv_bytes) = inner {
            let pair: (Key, Value) = (kv_bytes.0.to_vec().into(), kv_bytes.1.to_vec());
            kv_pairs.push(pair.into());
            if Some(kv_bytes.0) == end_it_key {
                break;
            }
        }
        if kv_pairs.len() >= limit as usize {
            break;
        }
    }
    Ok(kv_pairs.into_iter())
}

pub fn tx_scan_cf(
    txn: &Transaction<TransactionDB>,
    cf_handle: &impl AsColumnFamilyRef,
    range: impl Into<BoundRange>,
    limit: u32,
) -> Result<impl Iterator<Item=KvPair>> {
    let bound_range = range.into();
    let (start, end) = bound_range.into_keys();
    let start: Vec<u8> = start.into();
    let it = txn.prefix_iterator_cf(cf_handle, &start);
    let end_it_key = end
        .map(|e| {
            let e_vec: Vec<u8> = e.into();
            txn.prefix_iterator_cf(cf_handle, &e_vec)
        })
        .and_then(|mut it| it.next())
        .and_then(|res| res.ok()).map(|kv| kv.0);

    let mut kv_pairs: Vec<KvPair> = Vec::new();
    for inner in it {
        if let Ok(kv_bytes) = inner {
            let pair: (Key, Value) = (kv_bytes.0.to_vec().into(), kv_bytes.1.to_vec());
            kv_pairs.push(pair.into());
            if Some(kv_bytes.0) == end_it_key {
                break;
            }
        }
        if kv_pairs.len() >= limit as usize {
            break;
        }
    }
    Ok(kv_pairs.into_iter())
}

pub fn tx_scan_keys(
    txn: &Transaction<TransactionDB>,
    range: impl Into<BoundRange>,
    limit: u32,
) -> Result<impl Iterator<Item=Key>> {
    let bound_range = range.into();
    let (start, end) = bound_range.into_keys();
    let start: Vec<u8> = start.into();
    let it = txn.iterator(
        IteratorMode::From(&start, Direction::Forward)
    );
    let end_it_key = end
        .map(|e| {
            let e_vec: Vec<u8> = e.into();
            txn.iterator(
                IteratorMode::From(&e_vec, Direction::Forward)
            )
        })
        .and_then(|mut it| it.next())
        .and_then(|res| res.ok()).map(|kv| kv.0);

    let mut keys: Vec<Key> = Vec::new();
    for inner in it {
        if let Ok(kv_bytes) = inner {
            keys.push(kv_bytes.0.to_vec().into());
            if Some(kv_bytes.0) == end_it_key {
                break;
            }
        }
        if keys.len() >= limit as usize {
            break;
        }
    }
    Ok(keys.into_iter())
}

pub fn gen_next_meta_index() -> u16 {
    fetch_idx_and_add() % config_meta_key_number_or_default()
}

#[cfg(test)]
mod tests {
    use rocksdb::{Direction, IteratorMode, MultiThreaded, Options, TransactionDB, TransactionDBOptions, WriteBatchWithTransaction};
    use crate::rocks::kv::bound_range::BoundRange;
    use crate::rocks::kv::key::Key;
    use crate::rocks::{tx_scan, tx_scan_cf};

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

    #[test]
    fn test_cf() {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let mut transaction_opts = TransactionDBOptions::default();

        let db: TransactionDB<MultiThreaded> = TransactionDB::open_cf(&opts, &transaction_opts, ".rocksdb_store", ["cf1", "cf2"]).unwrap();

        let mut batch = WriteBatchWithTransaction::default();
        let cf1 = db.cf_handle("cf1").unwrap();
        let cf2 = db.cf_handle("cf2").unwrap();

        batch.put_cf(&cf1, b"aaa0", b"t1");
        batch.put_cf(&cf1, b"aaa1", b"t2");
        batch.put_cf(&cf2, b"aaa0123", b"t3");
        db.write(batch).unwrap();

        let txn = db.transaction();

        let start_key: Key = Key::from("aaa0".to_owned());
        let end_key: Key = Key::from("aaa1".to_owned());
        let bound_range: BoundRange = (start_key..end_key).into();

        let it = tx_scan_cf(&txn, &cf1, bound_range, 100).unwrap();
        for inner in it {
            println!("{inner:?}");
        }
    }
}