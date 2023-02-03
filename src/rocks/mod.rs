use lazy_static::lazy_static;
use crate::rocks::client::RocksClientWrapper;
use crate::rocks::errors::{REDIS_BACKEND_NOT_CONNECTED_ERR, RError};
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
    pub static ref ROCKS_RAW_CLIENT: Option<RocksClientWrapper> = RocksClientWrapper::new().ok();
}

pub fn set_instance_id(id: u64) {
    unsafe {
        INSTANCE_ID = id;
    }
}

pub fn get_instance_id() -> u64 {
    unsafe { INSTANCE_ID }
}

pub fn get_client<'a>() -> Result<&'a RocksClientWrapper> {
    ROCKS_RAW_CLIENT.as_ref().ok_or_else(|| REDIS_BACKEND_NOT_CONNECTED_ERR)
}

#[cfg(test)]
mod tests {
    use rocksdb::DB;

    #[test]
    fn test_rocksdb() {
        let db = DB::open_default(".rocksdb_store").unwrap();
        db.put(b"my key", b"my value").unwrap();
        match db.get(b"my key") {
            Ok(Some(value)) => println!("retrieved value {}", String::from_utf8(value).unwrap()),
            Ok(None) => println!("value not found"),
            Err(e) => println!("operational problem encountered: {}", e),
        }
        db.delete(b"my key").unwrap();
    }
}