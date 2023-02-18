use std::collections::HashMap;
use std::str;

use bytes::Bytes;

use crate::metrics::REMOVED_EXPIRED_KEY_COUNTER;
use crate::rocks::client::RocksRawClient;
use crate::rocks::encoding::{DataType, KeyDecoder};
use crate::rocks::errors::{RError, REDIS_WRONG_TYPE_ERR};
use crate::rocks::hash::HashCommand;
use crate::rocks::{RocksCommand, CF_NAME_META, KEY_ENCODER};
use crate::Frame;
use rocksdb::ColumnFamilyRef;

use crate::rocks::kv::key::Key;
use crate::rocks::kv::kvpair::KvPair;
use crate::rocks::kv::value::Value;
use crate::rocks::list::ListCommand;
use crate::rocks::set::SetCommand;
use crate::rocks::transaction::RocksTransaction;
use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::{
    key_is_expired, resp_array, resp_bulk, resp_err, resp_int, resp_nil, resp_ok, resp_str,
    ttl_from_timestamp,
};

pub struct StringCF<'a> {
    data_cf: ColumnFamilyRef<'a>,
}

impl<'a> StringCF<'a> {
    pub fn new(client: &'a RocksRawClient) -> Self {
        StringCF {
            data_cf: client.cf_handle(CF_NAME_META).unwrap(),
        }
    }
}

pub struct StringCommand<'a> {
    client: &'a RocksRawClient,
}

impl<'a> StringCommand<'a> {
    pub fn new(client: &'a RocksRawClient) -> Self {
        Self { client }
    }

    pub async fn get(&self, key: &str) -> RocksResult<Frame> {
        let client = self.client;
        let cfs = StringCF::new(client);
        let ekey = KEY_ENCODER.encode_string(key);
        match client.get(cfs.data_cf.clone(), ekey.clone())? {
            Some(val) => {
                let dt = KeyDecoder::decode_key_type(&val);
                if !matches!(dt, DataType::String) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(cfs.data_cf, ekey)?;
                    return Ok(resp_nil());
                }
                let data = KeyDecoder::decode_key_string_value(&val);
                Ok(resp_bulk(data))
            }
            None => Ok(Frame::Null),
        }
    }

    pub async fn get_type(&self, key: &str) -> RocksResult<Frame> {
        let client = self.client;
        let cfs = StringCF::new(client);
        let ekey = KEY_ENCODER.encode_string(key);
        match client.get(cfs.data_cf.clone(), ekey.clone())? {
            Some(val) => {
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(cfs.data_cf.clone(), ekey)?;
                    return Ok(resp_str(&DataType::Null.to_string()));
                }
                Ok(resp_str(&KeyDecoder::decode_key_type(&val).to_string()))
            }
            None => Ok(resp_str(&DataType::Null.to_string())),
        }
    }

    pub async fn strlen(&self, key: &str) -> RocksResult<Frame> {
        let client = self.client;
        let cfs = StringCF::new(client);
        let ekey = KEY_ENCODER.encode_string(key);
        match client.get(cfs.data_cf.clone(), ekey.clone())? {
            Some(val) => {
                let dt = KeyDecoder::decode_key_type(&val);
                if !matches!(dt, DataType::String) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(cfs.data_cf, ekey)?;
                    return Ok(resp_int(0));
                }
                let data = KeyDecoder::decode_key_string_value(&val);
                Ok(resp_int(data.len() as i64))
            }
            None => Ok(resp_int(0)),
        }
    }

    pub async fn put(self, key: &str, val: &Bytes, timestamp: i64) -> RocksResult<Frame> {
        let client = self.client;
        let cfs = StringCF::new(client);
        let ekey = KEY_ENCODER.encode_string(key);
        let eval = KEY_ENCODER.encode_string_value(&mut val.to_vec(), timestamp);
        client.put(cfs.data_cf, ekey, eval)?;
        Ok(resp_ok())
    }

    pub async fn batch_get(self, keys: &[String]) -> RocksResult<Frame> {
        let client = &self.client;
        let cfs = StringCF::new(client);
        let ekeys = KEY_ENCODER.encode_strings(keys);
        let result = client.batch_get(cfs.data_cf.clone(), ekeys.clone())?;
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
                            client
                                .del(cfs.data_cf.clone(), k)
                                .expect("remove outdated data failed");
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
        let client = self.client;
        let cfs = StringCF::new(client);
        client.batch_put(cfs.data_cf, kvs)?;
        Ok(resp_ok())
    }

    pub async fn put_not_exists(self, key: &str, value: &Bytes) -> RocksResult<Frame> {
        let client = self.client;
        let cfs = StringCF::new(client);
        let ekey = KEY_ENCODER.encode_string(key);
        let eval = KEY_ENCODER.encode_string_value(&mut value.to_vec(), -1);

        let resp = client.exec_txn(|txn| {
            match txn.get(cfs.data_cf.clone(), ekey.clone())? {
                Some(ref v) => {
                    let ttl = KeyDecoder::decode_key_ttl(v);
                    if key_is_expired(ttl) {
                        // no need to delete, just overwrite
                        txn.put(cfs.data_cf, ekey, eval)?;
                        Ok(1)
                    } else {
                        Ok(0)
                    }
                }
                None => {
                    txn.put(cfs.data_cf, ekey, eval)?;
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
        let client = self.client;
        let cfs = StringCF::new(client);
        let ekeys = KEY_ENCODER.encode_strings(keys);
        let result = client.batch_get(cfs.data_cf.clone(), ekeys.clone())?;
        let ret: HashMap<Key, Value> = result.into_iter().map(|pair| (pair.0, pair.1)).collect();
        let mut nums = 0;
        for k in ekeys {
            let data = ret.get(k.as_ref());
            if let Some(val) = data {
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(cfs.data_cf.clone(), k)?;
                } else {
                    nums += 1;
                }
            }
        }
        Ok(resp_int(nums as i64))
    }

    // TODO: All actions should in txn
    pub async fn incr(self, key: &str, step: i64) -> RocksResult<Frame> {
        let client = self.client;
        let cfs = StringCF::new(client);
        let ekey = KEY_ENCODER.encode_string(key);
        let the_key = ekey.clone();

        let resp = client.exec_txn(|txn| {
            match txn.get(cfs.data_cf.clone(), the_key.clone())? {
                Some(val) => {
                    let dt = KeyDecoder::decode_key_type(&val);
                    if !matches!(dt, DataType::String) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    // ttl saved in milliseconds
                    let ttl = KeyDecoder::decode_key_ttl(&val);
                    if key_is_expired(ttl) {
                        // delete key
                        txn.del(cfs.data_cf.clone(), the_key)?;
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
                None => Ok((0, None)),
            }
        })?;

        let (prev_int, _) = resp;

        let new_int = prev_int + step;
        let new_val = new_int.to_string();
        let eval = KEY_ENCODER.encode_string_value(&mut new_val.as_bytes().to_vec(), 0);
        client.put(cfs.data_cf, ekey, eval)?;
        Ok(resp_int(new_int))
    }

    pub async fn expire(self, key: &str, timestamp: i64) -> RocksResult<Frame> {
        let client = self.client;
        let cfs = StringCF::new(client);
        let key = key.to_owned();
        let timestamp = timestamp;
        let ekey = KEY_ENCODER.encode_string(&key);
        let resp = client.exec_txn(|txn| {
            match txn.get(cfs.data_cf.clone(), ekey.clone())? {
                Some(meta_value) => {
                    if timestamp == 0 {
                        return Ok(0);
                    }
                    let dt = KeyDecoder::decode_key_type(&meta_value);
                    match dt {
                        DataType::String => {
                            let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                            // check key expired
                            if key_is_expired(ttl) {
                                self.txn_expire_if_needed(txn, client, &ekey, &meta_value)?;
                                return Ok(0);
                            }
                            let value = KeyDecoder::decode_key_string_slice(&meta_value);
                            let new_meta_value = KEY_ENCODER.encode_string_slice(value, timestamp);
                            txn.put(cfs.data_cf.clone(), ekey, new_meta_value)?;
                            Ok(1)
                        }
                        DataType::Set => SetCommand::new(client).txn_expire(
                            txn,
                            client,
                            &key,
                            timestamp,
                            &meta_value,
                        ),
                        DataType::List => ListCommand::new(client).txn_expire(
                            txn,
                            client,
                            &key,
                            timestamp,
                            &meta_value,
                        ),
                        DataType::Hash => HashCommand::new(client).txn_expire(
                            txn,
                            client,
                            &key,
                            timestamp,
                            &meta_value,
                        ),
                        DataType::Zset => ZsetCommand::new(client).txn_expire(
                            txn,
                            client,
                            &key,
                            timestamp,
                            &meta_value,
                        ),
                        _ => Ok(0),
                    }
                }
                None => Ok(0),
            }
        });
        match resp {
            Ok(v) => Ok(resp_int(v)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn ttl(self, key: &str, is_millis: bool) -> RocksResult<Frame> {
        let client = self.client;
        let cfs = StringCF::new(client);
        let key = key.to_owned();
        let ekey = KEY_ENCODER.encode_string(&key);
        client.exec_txn(|txn| match txn.get(cfs.data_cf.clone(), ekey.clone())? {
            Some(meta_value) => {
                let dt = KeyDecoder::decode_key_type(&meta_value);
                let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                if key_is_expired(ttl) {
                    match dt {
                        DataType::String => {
                            self.txn_expire_if_needed(txn, client, &ekey, &meta_value)?;
                        }
                        DataType::Set => {
                            SetCommand::new(client).txn_expire_if_needed(txn, client, &key)?;
                        }
                        DataType::List => {
                            ListCommand::new(client).txn_expire_if_needed(txn, client, &key)?;
                        }
                        DataType::Hash => {
                            HashCommand::new(client).txn_expire_if_needed(txn, client, &key)?;
                        }
                        DataType::Zset => {
                            ZsetCommand::new(client).txn_expire_if_needed(txn, client, &key)?;
                        }
                        _ => {}
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
        })
    }

    pub async fn del(self, keys: &Vec<String>) -> RocksResult<Frame> {
        let client = self.client;
        let cfs = StringCF::new(client);
        let keys = keys.to_owned();
        let resp = client.exec_txn(|txn| {
            let ekeys = KEY_ENCODER.encode_strings(&keys);
            let ekey_map: HashMap<Key, String> = ekeys.clone().into_iter().zip(keys).collect();
            let cf = cfs.data_cf.clone();
            let pairs = txn.batch_get(cf, ekeys.clone())?;
            let dts: HashMap<Key, DataType> = pairs
                .into_iter()
                .map(|pair| (pair.0, KeyDecoder::decode_key_type(pair.1.as_slice())))
                .collect();

            let mut resp = 0;
            for ekey in ekeys {
                match dts.get(&ekey) {
                    Some(DataType::String) => {
                        txn.del(cfs.data_cf.clone(), ekey.clone())?;
                        resp += 1;
                    }
                    Some(DataType::Set) => {
                        SetCommand::new(client).txn_del(txn, client, &ekey_map[&ekey])?;
                        resp += 1;
                    }
                    Some(DataType::List) => {
                        ListCommand::new(client).txn_del(txn, client, &ekey_map[&ekey])?;
                        resp += 1;
                    }
                    Some(DataType::Hash) => {
                        HashCommand::new(client).txn_del(txn, client, &ekey_map[&ekey])?;
                        resp += 1;
                    }
                    Some(DataType::Zset) => {
                        ZsetCommand::new(client).txn_del(txn, client, &ekey_map[&ekey])?;
                        resp += 1;
                    }
                    _ => {}
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
    pub async fn scan(self, _start: &str, _count: u32, _regex: &str) -> RocksResult<Frame> {
        Ok(resp_array(vec![]))
    }

    fn txn_expire_if_needed(
        &self,
        txn: &RocksTransaction,
        client: &RocksRawClient,
        ekey: &Key,
        meta_value: &Value,
    ) -> RocksResult<i64> {
        let cfs = StringCF::new(client);
        let ttl = KeyDecoder::decode_key_ttl(meta_value);
        if key_is_expired(ttl) {
            txn.del(cfs.data_cf.clone(), ekey.to_owned())?;
            REMOVED_EXPIRED_KEY_COUNTER
                .with_label_values(&["string"])
                .inc();
            return Ok(1);
        }
        Ok(0)
    }
}
