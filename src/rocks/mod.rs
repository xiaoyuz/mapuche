use std::sync::Arc;
use lazy_static::lazy_static;
use rocksdb::{TransactionDB};
use crate::rocks::client::RocksRawClient;
use crate::rocks::errors::{RError};
use crate::rocks::encoding::KeyEncoder;

pub mod client;
pub mod errors;
pub mod string;
pub mod kv;
pub mod encoding;

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

#[cfg(test)]
mod tests {
    use rocksdb::{DB, WriteBatch};

    #[test]
    fn test_rocksdb() {
        let db = DB::open_default(".rocksdb_store").unwrap();
        db.put(b"my key", b"my value").unwrap();
        match db.get(b"my key") {
            Ok(Some(value)) => println!("retrieved value {}", String::from_utf8(value).unwrap()),
            Ok(None) => println!("value not found"),
            Err(e) => println!("operational problem encountered: {e}"),
        }
        db.delete(b"my key").unwrap();

        let mut batch = WriteBatch::default();
        batch.put(b"test000001", b"t1");
        batch.put(b"test000002", b"t2");
        batch.put(b"test000010", b"t3");
        batch.put(b"test001111", b"t3");
        batch.put(b"zzzzz", b"a1");
        db.write(batch).unwrap();

        let it = db.prefix_iterator(b"test000010");
        for inner in it {
            println!("{:?}", inner.unwrap());
        }
    }
}