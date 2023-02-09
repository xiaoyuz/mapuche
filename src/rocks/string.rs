use std::collections::HashMap;
use std::str;
use bytes::Bytes;
use regex::bytes::Regex;
use rocksdb::{Transaction, TransactionDB};
use crate::Frame;
use crate::rocks::{tx_scan, get_client, KEY_ENCODER};
use crate::rocks::encoding::{DataType, KeyDecoder};
use crate::rocks::errors::{REDIS_WRONG_TYPE_ERR, RError};
use crate::rocks::kv::bound_range::BoundRange;

use crate::rocks::kv::key::Key;
use crate::rocks::kv::kvpair::KvPair;
use crate::rocks::kv::value::Value;
use crate::rocks::Result as RocksResult;
use crate::utils::{key_is_expired, resp_array, resp_bulk, resp_err, resp_int, resp_nil, resp_ok, resp_str, ttl_from_timestamp};

#[derive(Clone)]
pub struct StringCommand;

impl StringCommand {

    pub async fn get(&self, key: &str) -> RocksResult<Frame> {
        let client = get_client();
        let ekey = KEY_ENCODER.encode_txn_kv_string(key);
        match client.get(ekey.clone())? {
            Some(val) => {
                let dt = KeyDecoder::decode_key_type(&val);
                if !matches!(dt, DataType::String) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(ekey)?;
                    return Ok(resp_nil());
                }
                let data = KeyDecoder::decode_key_string_value(&val);
                Ok(resp_bulk(data))
            }
            None => Ok(Frame::Null),
        }
    }

    pub async fn get_type(&self, key: &str) -> RocksResult<Frame> {
        let client = get_client();
        let ekey = KEY_ENCODER.encode_txn_kv_string(key);
        match client.get(ekey.clone())? {
            Some(val) => {
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(ekey)?;
                    return Ok(resp_str(&DataType::Null.to_string()));
                }
                Ok(resp_str(&KeyDecoder::decode_key_type(&val).to_string()))
            }
            None => Ok(resp_str(&DataType::Null.to_string())),
        }
    }

    pub async fn strlen(&self, key: &str) -> RocksResult<Frame> {
        let client = get_client();
        let ekey = KEY_ENCODER.encode_txn_kv_string(key);
        match client.get(ekey.clone())? {
            Some(val) => {
                let dt = KeyDecoder::decode_key_type(&val);
                if !matches!(dt, DataType::String) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(ekey)?;
                    return Ok(resp_int(0));
                }
                let data = KeyDecoder::decode_key_string_value(&val);
                Ok(resp_int(data.len() as i64))
            }
            None => Ok(resp_int(0)),
        }
    }

    pub async fn put(self, key: &str, val: &Bytes, timestamp: i64) -> RocksResult<Frame> {
        let client = get_client();
        let ekey = KEY_ENCODER.encode_txn_kv_string(key);
        let eval = KEY_ENCODER.encode_txn_kv_string_value(&mut val.to_vec(), timestamp);
        client.put(ekey, eval)?;
        Ok(resp_ok())
    }

    pub async fn batch_get(self, keys: &[String]) -> RocksResult<Frame> {
        let client = get_client();
        let ekeys = KEY_ENCODER.encode_raw_kv_strings(keys);
        let result = client.batch_get(ekeys.clone())?;
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
                            client.del(k).expect("remove outdated data failed");
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

    pub async fn batch_put(self, kvs: Vec<KvPair>) -> RocksResult<Frame> {
        let client = get_client();
        client.batch_put(kvs)?;
        Ok(resp_ok())
    }

    pub async fn put_not_exists(self, key: &str, value: &Bytes) -> RocksResult<Frame> {
        let client = get_client();
        let ekey = KEY_ENCODER.encode_txn_kv_string(key);
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
        });

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

    pub async fn exists(self, keys: &[String]) -> RocksResult<Frame> {
        let client = get_client();
        let ekeys = KEY_ENCODER.encode_raw_kv_strings(keys);
        let result = client.batch_get(ekeys.clone())?;
        let ret: HashMap<Key, Value> = result.into_iter().map(|pair| (pair.0, pair.1)).collect();
        let mut nums = 0;
        for k in ekeys {
            let data = ret.get(k.as_ref());
            if let Some(val) = data {
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(k)?;
                } else {
                    nums += 1;
                }
            }
        }
        Ok(resp_int(nums as i64))
    }

    pub async fn incr(self, key: &str, step: i64) -> RocksResult<Frame> {
        let client = get_client();
        let ekey = KEY_ENCODER.encode_txn_kv_string(key);
        let the_key = ekey.clone();

        let resp = client.exec_txn(|txn| {
            match txn.get(the_key.clone())? {
                Some(val) => {
                    let dt = KeyDecoder::decode_key_type(&val);
                    if !matches!(dt, DataType::String) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    // ttl saved in milliseconds
                    let ttl = KeyDecoder::decode_key_ttl(&val);
                    if key_is_expired(ttl) {
                        // delete key
                        txn.delete(the_key)?;
                        Ok((0, None))
                    } else {
                        let current_value = KeyDecoder::decode_key_string_slice(&val);
                        let prev_int = str::from_utf8(current_value)
                            .map_err(RError::is_not_integer_error)?
                            .parse::<i64>()?;
                        let prev = Some(val.clone());
                        Ok((prev_int, prev))
                    }
                }
                None => {
                    Ok((0, None))
                }
            }
        })?;

        let (prev_int, _) = resp;

        let new_int = prev_int + step;
        let new_val = new_int.to_string();
        let eval = KEY_ENCODER.encode_txn_kv_string_value(&mut new_val.as_bytes().to_vec(), 0);
        client.put(ekey, eval)?;
        Ok(resp_int(new_int))
    }

    pub async fn string_del(self, key: &str) -> RocksResult<()> {
        let client = get_client();
        let ekey = KEY_ENCODER.encode_txn_kv_string(key);
        client.del(ekey)
    }

    pub fn txn_string_del(&self, txn: &Transaction<TransactionDB>, key: &str) -> RocksResult<()> {
        let ekey = KEY_ENCODER.encode_txn_kv_string(key);
        txn.delete(ekey).map_err(|e| e.into())
    }

    pub fn txn_expire_if_needed(self, txn: &Transaction<TransactionDB>, key: &str) -> RocksResult<()> {
        let ekey = KEY_ENCODER.encode_txn_kv_string(key);
        if let Some(v) = txn.get(ekey.clone())? {
            let ttl = KeyDecoder::decode_key_ttl(&v);
            if key_is_expired(ttl) {
                txn.delete(ekey)?;
            }
        }
        Ok(())
    }

    pub async fn expire(self, key: &str, timestamp: i64) -> RocksResult<Frame> {
        let client = get_client();
        let key = key.to_owned();
        let timestamp = timestamp;
        let ekey = KEY_ENCODER.encode_txn_kv_string(&key);
        let resp = client.exec_txn(move |txn| {
            match txn.get(ekey.clone())? {
                Some(meta_value) => {
                    let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                    if timestamp == 0 && ttl == 0 {
                        // this is a persist command
                        // check old ttl first, no need to perform op
                        return Ok(0);
                    }
                    let dt = KeyDecoder::decode_key_type(&meta_value);
                    let _version = KeyDecoder::decode_key_version(&meta_value);
                    match dt {
                        DataType::String => {
                            // check key expired
                            if key_is_expired(ttl) {
                                self.txn_expire_if_needed(txn, &key)?;
                                return Ok(0);
                            }
                            let value = KeyDecoder::decode_key_string_slice(&meta_value);
                            let new_meta_value =
                                KEY_ENCODER.encode_txn_kv_string_slice(value, timestamp);
                            txn.put(ekey, new_meta_value)?;
                            Ok(1)
                        }
                        _ => {
                            // TODO: add all types
                            Ok(0)
                        }
                    }
                }
                None => Ok(0)
            }
        });
        match resp {
            Ok(v) => Ok(resp_int(v)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn ttl(self, key: &str, is_millis: bool) -> RocksResult<Frame> {
        let client = get_client();
        let key = key.to_owned();
        let ekey = KEY_ENCODER.encode_txn_kv_string(&key);
        client.exec_txn(move |txn| {
            match txn.get(ekey.clone())? {
                Some(meta_value) => {
                    let dt = KeyDecoder::decode_key_type(&meta_value);
                    let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                    if key_is_expired(ttl) {
                        match dt {
                            DataType::String => {
                                self.txn_expire_if_needed(txn, &key)?;
                            }
                            _ => {
                                // TODO: add all types
                            }
                        }
                        return Ok(resp_int(-2));
                    }
                    if ttl == 0 {
                        Ok(resp_int(-1))
                    } else {
                        let mut ttl = ttl_from_timestamp(ttl);
                        if !is_millis {
                            ttl /= 1000;
                        }
                        Ok(resp_int(ttl))
                    }
                }
                None => Ok(resp_int(-2)),
            }
        })
    }

    pub async fn del(self, keys: &Vec<String>) -> RocksResult<Frame> {
        let client = get_client();
        let keys = keys.to_owned();
        let keys_len = keys.len();
        let resp = client.exec_txn(move |txn| {
            let mut dts = Vec::with_capacity(keys_len);
            let ekeys = KEY_ENCODER.encode_raw_kv_strings(&keys);

            let values = txn.multi_get(&ekeys);
            for i in 0..ekeys.len() {
                match values.get(i) {
                    Some(Ok(Some(v))) => dts.push(KeyDecoder::decode_key_type(v)),
                    _ => dts.push(DataType::Null),
                }
            }

            let mut resp = 0;
            for idx in 0..keys_len {
                match dts[idx] {
                    DataType::String => {
                        self.clone().txn_string_del(txn, &keys[idx])?;
                        resp += 1;
                    }
                    _ => {
                        // TODO add all types
                    }
                }
            }
            Ok(resp)
        });
        match resp {
            Ok(v) => Ok(resp_int(v)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    // TODO
    pub async fn scan(
        self,
        start: &str,
        count: u32,
        regex: &str,
    ) -> RocksResult<Frame> {
        let client = get_client();
        let ekey = KEY_ENCODER.encode_txn_kv_string(start);
        let re = Regex::new(regex).unwrap();
        client.exec_txn(move |txn| {
            let mut keys = vec![];
            let mut retrieved_key_count = 0;
            let mut next_key = vec![];
            let mut left_bound = ekey.clone();

            // set to a non-zore value before loop
            let mut last_round_iter_count = 1;
            while retrieved_key_count < count as usize {
                if last_round_iter_count == 0 {
                    next_key = vec![];
                    break;
                }

                let range = left_bound.clone()..KEY_ENCODER.encode_txn_kv_keyspace_end();
                let bound_range: BoundRange = range.into();

                // the iterator will scan all keyspace include sub metakey and datakey
                let iter = tx_scan(txn, bound_range, 100)?;

                // reset count to zero
                last_round_iter_count = 0;
                for kv in iter {
                    // skip the left bound key, this should be exclusive
                    if kv.0 == left_bound {
                        continue;
                    }
                    left_bound = kv.0.clone();
                    // left bound key is exclusive
                    last_round_iter_count += 1;
                    let (userkey, is_meta_key) =
                        KeyDecoder::decode_key_userkey_from_metakey(&kv.0);

                    // skip it if it is not a meta key
                    if !is_meta_key {
                        continue;
                    }

                    let ttl = KeyDecoder::decode_key_ttl(&kv.1);
                    if retrieved_key_count == (count - 1) as usize {
                        next_key = userkey.clone();
                        retrieved_key_count += 1;
                        if re.is_match(&userkey) && !key_is_expired(ttl) {
                            keys.push(resp_bulk(userkey));
                        }
                        break;
                    }
                    retrieved_key_count += 1;
                    if re.is_match(&userkey) {
                        keys.push(resp_bulk(userkey));
                    }
                }
            }
            let resp_next_key = resp_bulk(next_key);
            let resp_keys = resp_array(keys);

            Ok(resp_array(vec![resp_next_key, resp_keys]))
        })
    }
}