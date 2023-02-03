use bytes::Bytes;
use crate::Frame;
use crate::rocks::{get_client, KEY_ENCODER};
use crate::rocks::encoding::{DataType, KeyDecoder};
use crate::rocks::Result as RocksResult;
use crate::utils::{resp_ok, resp_str};

pub struct StringCommand;

impl StringCommand {
    fn new() -> Self {
        Self
    }

    pub async fn raw_kv_get(&self, key: &str) -> RocksResult<Frame> {
        let client = get_client()?;
        let ekey = KEY_ENCODER.encode_raw_kv_string(key);
        match client.get(ekey).await? {
            Some(val) => Ok(Frame::Bulk(val.into())),
            None => Ok(Frame::Null),
        }
    }

    pub async fn raw_kv_type(&self, key: &str) -> RocksResult<Frame> {
        let client = get_client()?;
        let ekey = KEY_ENCODER.encode_raw_kv_string(key);

        match client.get(ekey).await? {
            Some(val) => Ok(resp_str(&KeyDecoder::decode_key_type(&val).to_string())),
            None => Ok(resp_str(&DataType::Null.to_string())),
        }
    }

    pub async fn raw_kv_strlen(&self, key: &str) -> RocksResult<Frame> {
        let client = get_client()?;
        let ekey = KEY_ENCODER.encode_raw_kv_string(key);
        match client.get(ekey).await? {
            Some(val) => Ok(Frame::Integer(val.len() as u64)),
            None => Ok(Frame::Integer(0)),
        }
    }

    pub async fn raw_kv_put(self, key: &str, val: &Bytes) -> RocksResult<Frame> {
        let client = get_client()?;
        let ekey = KEY_ENCODER.encode_raw_kv_string(key);
        client.put(ekey, val.to_vec()).await?;
        Ok(resp_ok())
    }
}