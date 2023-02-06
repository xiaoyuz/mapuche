use std::collections::HashMap;
use std::str;
use bytes::Bytes;
use crate::Frame;
use crate::rocks::{get_client, KEY_ENCODER};
use crate::rocks::encoding::{DataType, KeyDecoder};
use crate::rocks::errors::{REDIS_WRONG_TYPE_ERR, RError};
use crate::rocks::kv::key::Key;
use crate::rocks::kv::kvpair::KvPair;
use crate::rocks::kv::value::Value;
use crate::rocks::Result as RocksResult;
use crate::utils::{key_is_expired, resp_bulk, resp_err, resp_int, resp_nil, resp_ok, resp_str};

pub struct StringCommand;

impl StringCommand {
    fn new() -> Self {
        Self
    }

    pub async fn raw_kv_get(&self, key: &str) -> RocksResult<Frame> {
        let client = get_client();
        let ekey = KEY_ENCODER.encode_raw_kv_string(key);
        match client.get(ekey.clone()).await? {
            Some(val) => {
                let dt = KeyDecoder::decode_key_type(&val);
                if !matches!(dt, DataType::String) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(ekey);
                    return Ok(resp_nil());
                }
                let data = KeyDecoder::decode_key_string_value(&val);
                Ok(resp_bulk(data))
            }
            None => Ok(Frame::Null),
        }
    }

    pub async fn raw_kv_type(&self, key: &str) -> RocksResult<Frame> {
        let client = get_client();
        let ekey = KEY_ENCODER.encode_raw_kv_string(key);
        match client.get(ekey.clone()).await? {
            Some(val) => {
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(ekey);
                    return Ok(resp_str(&DataType::Null.to_string()));
                }
                Ok(resp_str(&KeyDecoder::decode_key_type(&val).to_string()))
            }
            None => Ok(resp_str(&DataType::Null.to_string())),
        }
    }

    pub async fn raw_kv_strlen(&self, key: &str) -> RocksResult<Frame> {
        let client = get_client();
        let ekey = KEY_ENCODER.encode_raw_kv_string(key);
        match client.get(ekey.clone()).await? {
            Some(val) => {
                let dt = KeyDecoder::decode_key_type(&val);
                if !matches!(dt, DataType::String) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(ekey);
                    return Ok(resp_int(0));
                }
                let data = KeyDecoder::decode_key_string_value(&val);
                Ok(resp_int(data.len() as i64))
            }
            None => Ok(resp_int(0)),
        }
    }

    pub async fn raw_kv_put(self, key: &str, val: &Bytes, timestamp: i64) -> RocksResult<Frame> {
        let client = get_client();
        let ekey = KEY_ENCODER.encode_raw_kv_string(key);
        let eval = KEY_ENCODER.encode_txn_kv_string_value(&mut val.to_vec(), timestamp);
        client.put(ekey, eval).await?;
        Ok(resp_ok())
    }

    pub async fn raw_kv_batch_get(self, keys: &[String]) -> RocksResult<Frame> {
        let client = get_client();
        let ekeys = KEY_ENCODER.encode_raw_kv_strings(keys);
        let result = client.batch_get(ekeys.clone()).await?;
        let ret: HashMap<Key, Value> = result.into_iter().map(|pair| (pair.0, pair.1)).collect();

        let values: Vec<Frame> = ekeys
            .into_iter()
            .map(|k| {
                let data = ret.get(k.as_ref());
                match data {
                    Some(val) => {
                        // ttl saved in milliseconds
                        let ttl = KeyDecoder::decode_key_ttl(val);
                        if key_is_expired(ttl) {
                            // delete key
                            client.del(k);
                            Frame::Null
                        } else {
                            let data = KeyDecoder::decode_key_string_value(val);
                            resp_bulk(data)
                        }
                    }
                    None => Frame::Null,
                }
            })
            .collect();
        Ok(Frame::Array(values))
    }

    pub async fn raw_kv_batch_put(self, kvs: Vec<KvPair>) -> RocksResult<Frame> {
        let client = get_client();
        client.batch_put(kvs).await?;
        Ok(resp_ok())
    }

    // TODO: CAS need atomic
    pub async fn raw_kv_put_not_exists(self, key: &str, value: &Bytes) -> RocksResult<Frame> {
        let client = get_client();
        let ekey = KEY_ENCODER.encode_raw_kv_string(key);
        let eval = KEY_ENCODER.encode_txn_kv_string_value(&mut value.to_vec(), -1);

        let resp = client.exec_txn(|txn| {
            match txn.get(ekey.clone())? {
                Some(ref v) => {
                    let ttl = KeyDecoder::decode_key_ttl(v);
                    if key_is_expired(ttl) {
                        // no need to delete, just overwrite
                        txn.put(ekey, eval)?;
                        Ok(1)
                    } else {
                        Ok(0)
                    }
                }
                None => {
                    txn.put(ekey, eval)?;
                    Ok(1)
                }
            }
        }).await;

        match resp {
            Ok(n) => {
                if n == 0 {
                    Ok(resp_nil())
                } else {
                    Ok(resp_ok())
                }
            }
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn raw_kv_exists(self, keys: &[String]) -> RocksResult<Frame> {
        let client = get_client();
        let ekeys = KEY_ENCODER.encode_raw_kv_strings(keys);
        let result = client.batch_get(ekeys.clone()).await?;
        let ret: HashMap<Key, Value> = result.into_iter().map(|pair| (pair.0, pair.1)).collect();
        let mut nums = 0;
        for k in ekeys {
            let data = ret.get(k.as_ref());
            if let Some(val) = data {
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(k);
                } else {
                    nums += 1;
                }
            }
        }
        Ok(resp_int(nums as i64))
    }

    pub async fn raw_kv_incr(self, key: &str, step: i64) -> RocksResult<Frame> {
        let client = get_client();
        let ekey = KEY_ENCODER.encode_raw_kv_string(key);
        let the_key = ekey.clone();
        let prev: Option<Vec<u8>>;
        let prev_int: i64;

        match client.get(the_key.clone()).await? {
            Some(val) => {
                let dt = KeyDecoder::decode_key_type(&val);
                if !matches!(dt, DataType::String) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(the_key);
                    prev_int = 0;
                    prev = None;
                } else {
                    let current_value = KeyDecoder::decode_key_string_slice(&val);
                    prev_int = str::from_utf8(current_value)
                        .map_err(RError::is_not_integer_error)?
                        .parse::<i64>()?;
                    prev = Some(val.clone());
                }
            }
            None => {
                prev = None;
                prev_int = 0;
            }
        }

        let new_int = prev_int + step;
        let new_val = new_int.to_string();
        let eval = KEY_ENCODER.encode_txn_kv_string_value(&mut new_val.as_bytes().to_vec(), 0);
        client.put(ekey, eval).await?;
        Ok(resp_int(new_int))
    }
}