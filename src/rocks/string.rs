use std::collections::HashMap;
use std::str;

use bytes::Bytes;
use glob::Pattern;
use regex::bytes::Regex;

use crate::db::DBInner;
use crate::rocks::client::RocksClient;
use crate::rocks::encoding::{DataType, KeyDecoder};
use crate::rocks::errors::{RError, REDIS_WRONG_TYPE_ERR};
use crate::rocks::hash::HashCommand;
use crate::rocks::kv::bound_range::BoundRange;
use crate::rocks::{TxnCommand, CF_NAME_META};
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
    meta_cf: ColumnFamilyRef<'a>,
}

impl<'a> StringCF<'a> {
    pub fn new(client: &'a RocksClient) -> Self {
        StringCF {
            meta_cf: client.cf_handle(CF_NAME_META).unwrap(),
        }
    }
}

pub struct StringCommand<'a> {
    inner_db: &'a DBInner,
}

impl<'a> StringCommand<'a> {
    pub fn new(inner_db: &'a DBInner) -> Self {
        Self { inner_db }
    }

    pub async fn get(&self, key: &str) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = StringCF::new(client);
        let ekey = self.inner_db.key_encoder.encode_string(key);
        match client.get(cfs.meta_cf.clone(), ekey.clone())? {
            Some(val) => {
                let dt = KeyDecoder::decode_key_type(&val);
                if !matches!(dt, DataType::String) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(cfs.meta_cf, ekey)?;
                    return Ok(resp_nil());
                }
                let data = KeyDecoder::decode_key_string_value(&val);
                Ok(resp_bulk(data))
            }
            None => Ok(Frame::Null),
        }
    }

    pub async fn get_type(&self, key: &str) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = StringCF::new(client);
        let ekey = self.inner_db.key_encoder.encode_string(key);
        match client.get(cfs.meta_cf.clone(), ekey.clone())? {
            Some(val) => {
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(cfs.meta_cf.clone(), ekey)?;
                    return Ok(resp_str(&DataType::Null.to_string()));
                }
                Ok(resp_str(&KeyDecoder::decode_key_type(&val).to_string()))
            }
            None => Ok(resp_str(&DataType::Null.to_string())),
        }
    }

    pub async fn strlen(&self, key: &str) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = StringCF::new(client);
        let ekey = self.inner_db.key_encoder.encode_string(key);
        match client.get(cfs.meta_cf.clone(), ekey.clone())? {
            Some(val) => {
                let dt = KeyDecoder::decode_key_type(&val);
                if !matches!(dt, DataType::String) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(cfs.meta_cf, ekey)?;
                    return Ok(resp_int(0));
                }
                let data = KeyDecoder::decode_key_string_value(&val);
                Ok(resp_int(data.len() as i64))
            }
            None => Ok(resp_int(0)),
        }
    }

    pub async fn put(self, key: &str, val: &Bytes, timestamp: i64) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = StringCF::new(client);
        let ekey = self.inner_db.key_encoder.encode_string(key);
        let eval = self
            .inner_db
            .key_encoder
            .encode_string_value(&mut val.to_vec(), timestamp);
        client.put(cfs.meta_cf, ekey, eval)?;
        Ok(resp_ok())
    }

    pub async fn batch_get(self, keys: &[String]) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = StringCF::new(client);
        let ekeys = self.inner_db.key_encoder.encode_strings(keys);
        let result = client.batch_get(cfs.meta_cf.clone(), ekeys.clone())?;
        let ret: HashMap<Key, Value> = result.into_iter().map(|pair| (pair.0, pair.1)).collect();

        let values: Vec<Frame> = ekeys
            .into_iter()
            .map(|k| {
                let data = ret.get(&k);
                match data {
                    Some(val) => {
                        // ttl saved in milliseconds
                        let ttl = KeyDecoder::decode_key_ttl(val);
                        if key_is_expired(ttl) {
                            // delete key
                            client
                                .del(cfs.meta_cf.clone(), k)
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
        let client = &self.inner_db.client;
        let cfs = StringCF::new(client);
        client.batch_put(cfs.meta_cf, kvs)?;
        Ok(resp_ok())
    }

    pub async fn put_not_exists(self, key: &str, value: &Bytes) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = StringCF::new(client);
        let ekey = self.inner_db.key_encoder.encode_string(key);
        let eval = self
            .inner_db
            .key_encoder
            .encode_string_value(&mut value.to_vec(), -1);

        let resp = client.exec_txn(|txn| {
            match txn.get_for_update(cfs.meta_cf.clone(), ekey.clone())? {
                Some(ref v) => {
                    let ttl = KeyDecoder::decode_key_ttl(v);
                    if key_is_expired(ttl) {
                        // no need to delete, just overwrite
                        txn.put(cfs.meta_cf, ekey, eval)?;
                        Ok(1)
                    } else {
                        Ok(0)
                    }
                }
                None => {
                    txn.put(cfs.meta_cf, ekey, eval)?;
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
        let client = &self.inner_db.client;
        let cfs = StringCF::new(client);
        let ekeys = self.inner_db.key_encoder.encode_strings(keys);
        let result = client.batch_get(cfs.meta_cf.clone(), ekeys.clone())?;
        let ret: HashMap<Key, Value> = result.into_iter().map(|pair| (pair.0, pair.1)).collect();
        let mut nums = 0;
        for k in ekeys {
            let data = ret.get(&k);
            if let Some(val) = data {
                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(val);
                if key_is_expired(ttl) {
                    // delete key
                    client.del(cfs.meta_cf.clone(), k)?;
                } else {
                    nums += 1;
                }
            }
        }
        Ok(resp_int(nums as i64))
    }

    // TODO: All actions should in txn
    pub async fn incr(self, key: &str, step: i64) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = StringCF::new(client);
        let ekey = self.inner_db.key_encoder.encode_string(key);
        let the_key = ekey.clone();

        client.exec_txn(|txn| {
            let pair = match txn.get_for_update(cfs.meta_cf.clone(), the_key.clone())? {
                Some(val) => {
                    let dt = KeyDecoder::decode_key_type(&val);
                    if !matches!(dt, DataType::String) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    // ttl saved in milliseconds
                    let ttl = KeyDecoder::decode_key_ttl(&val);
                    if key_is_expired(ttl) {
                        // delete key
                        txn.del(cfs.meta_cf.clone(), the_key)?;
                        (0, None)
                    } else {
                        let current_value = KeyDecoder::decode_key_string_slice(&val);
                        let prev_int = str::from_utf8(current_value)
                            .map_err(RError::is_not_integer_error)?
                            .parse::<i64>()?;
                        let prev = Some(val.clone());
                        (prev_int, prev)
                    }
                }
                None => (0, None),
            };

            let (prev_int, _) = pair;

            let new_int = prev_int + step;
            let new_val = new_int.to_string();
            let eval = self
                .inner_db
                .key_encoder
                .encode_string_value(&mut new_val.as_bytes().to_vec(), 0);
            txn.put(cfs.meta_cf, ekey, eval)?;
            Ok(resp_int(new_int))
        })
    }

    pub async fn expire(self, key: &str, timestamp: i64) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = StringCF::new(client);
        let key = key.to_owned();
        let timestamp = timestamp;
        let ekey = self.inner_db.key_encoder.encode_string(&key);
        let resp = client.exec_txn(|txn| {
            match txn.get_for_update(cfs.meta_cf.clone(), ekey.clone())? {
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
                                self.txn_expire_if_needed(txn, &ekey, &meta_value)?;
                                return Ok(0);
                            }
                            let value = KeyDecoder::decode_key_string_slice(&meta_value);
                            let new_meta_value = self
                                .inner_db
                                .key_encoder
                                .encode_string_slice(value, timestamp);
                            txn.put(cfs.meta_cf.clone(), ekey, new_meta_value)?;
                            Ok(1)
                        }
                        DataType::Set => SetCommand::new(self.inner_db).txn_expire(
                            txn,
                            &key,
                            timestamp,
                            &meta_value,
                        ),
                        DataType::List => ListCommand::new(self.inner_db).txn_expire(
                            txn,
                            &key,
                            timestamp,
                            &meta_value,
                        ),
                        DataType::Hash => HashCommand::new(self.inner_db).txn_expire(
                            txn,
                            &key,
                            timestamp,
                            &meta_value,
                        ),
                        DataType::Zset => ZsetCommand::new(self.inner_db).txn_expire(
                            txn,
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
        let client = &self.inner_db.client;
        let cfs = StringCF::new(client);
        let key = key.to_owned();
        let ekey = self.inner_db.key_encoder.encode_string(&key);
        client.exec_txn(|txn| match txn.get(cfs.meta_cf.clone(), ekey.clone())? {
            Some(meta_value) => {
                let dt = KeyDecoder::decode_key_type(&meta_value);
                let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                if key_is_expired(ttl) {
                    match dt {
                        DataType::String => {
                            self.txn_expire_if_needed(txn, &ekey, &meta_value)?;
                        }
                        DataType::Set => {
                            SetCommand::new(self.inner_db).txn_expire_if_needed(txn, &key)?;
                        }
                        DataType::List => {
                            ListCommand::new(self.inner_db).txn_expire_if_needed(txn, &key)?;
                        }
                        DataType::Hash => {
                            HashCommand::new(self.inner_db).txn_expire_if_needed(txn, &key)?;
                        }
                        DataType::Zset => {
                            ZsetCommand::new(self.inner_db).txn_expire_if_needed(txn, &key)?;
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
        let client = &self.inner_db.client;
        let cfs = StringCF::new(client);
        let keys = keys.to_owned();
        let resp = client.exec_txn(|txn| {
            let ekeys = self.inner_db.key_encoder.encode_strings(&keys);
            let ekey_map: HashMap<Key, String> = ekeys.clone().into_iter().zip(keys).collect();
            let cf = cfs.meta_cf.clone();
            let pairs = txn.batch_get(cf, ekeys.clone())?;
            let dts: HashMap<Key, DataType> = pairs
                .into_iter()
                .map(|pair| (pair.0, KeyDecoder::decode_key_type(pair.1.as_slice())))
                .collect();

            let mut resp = 0;
            for ekey in ekeys {
                match dts.get(&ekey) {
                    Some(DataType::String) => {
                        txn.del(cfs.meta_cf.clone(), ekey.clone())?;
                        resp += 1;
                    }
                    Some(DataType::Set) => {
                        SetCommand::new(self.inner_db).txn_del(txn, &ekey_map[&ekey])?;
                        resp += 1;
                    }
                    Some(DataType::List) => {
                        ListCommand::new(self.inner_db).txn_del(txn, &ekey_map[&ekey])?;
                        resp += 1;
                    }
                    Some(DataType::Hash) => {
                        HashCommand::new(self.inner_db).txn_del(txn, &ekey_map[&ekey])?;
                        resp += 1;
                    }
                    Some(DataType::Zset) => {
                        ZsetCommand::new(self.inner_db).txn_del(txn, &ekey_map[&ekey])?;
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

    pub async fn keys(self, regex: &str) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = StringCF::new(client);
        let ekey = self.inner_db.key_encoder.encode_string("");
        let re = Pattern::new(regex).unwrap();

        client.exec_txn(|txn| {
            let mut keys = vec![];
            let mut last_round_iter_count = 1;

            let mut left_bound = ekey.clone();

            loop {
                if last_round_iter_count == 0 {
                    break;
                }
                let range = left_bound.clone()..self.inner_db.key_encoder.encode_keyspace_end();
                let bound_range: BoundRange = range.into();

                let iter = txn.scan(cfs.meta_cf.clone(), bound_range, 100)?;
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

                    let (userkey, is_meta_key) = KeyDecoder::decode_key_userkey_from_metakey(&kv.0);

                    // skip it if it is not a meta key
                    if !is_meta_key {
                        continue;
                    }

                    let ttl = KeyDecoder::decode_key_ttl(&kv.1);
                    // delete it if it is expired
                    if key_is_expired(ttl) {
                        continue;
                    }
                    if re.matches(str::from_utf8(&userkey).unwrap()) {
                        keys.push(resp_bulk(userkey));
                    }
                }
            }
            Ok(resp_array(keys))
        })
    }

    pub async fn scan(self, start: &str, count: u32, regex: &str) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = StringCF::new(client);
        let ekey = self.inner_db.key_encoder.encode_string(start);
        let re = Regex::new(regex).unwrap();

        client.exec_txn(|txn| {
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

                let range = left_bound.clone()..self.inner_db.key_encoder.encode_keyspace_end();
                let bound_range: BoundRange = range.into();

                // the iterator will scan all keyspace include sub metakey and datakey
                let iter = txn.scan(cfs.meta_cf.clone(), bound_range, 100)?;

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

                    let (userkey, is_meta_key) = KeyDecoder::decode_key_userkey_from_metakey(&kv.0);

                    // skip it if it is not a meta key
                    if !is_meta_key {
                        continue;
                    }

                    let ttl = KeyDecoder::decode_key_ttl(&kv.1);
                    // delete it if it is expired
                    if key_is_expired(ttl) {
                        continue;
                    }
                    if retrieved_key_count == (count - 1) as usize {
                        next_key = userkey.clone();
                        retrieved_key_count += 1;
                        if re.is_match(&userkey) {
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

    fn txn_expire_if_needed(
        &self,
        txn: &RocksTransaction,
        ekey: &Key,
        meta_value: &Value,
    ) -> RocksResult<i64> {
        let cfs = StringCF::new(&self.inner_db.client);
        let ttl = KeyDecoder::decode_key_ttl(meta_value);
        if key_is_expired(ttl) {
            txn.del(cfs.meta_cf.clone(), ekey.to_owned())?;
            return Ok(1);
        }
        Ok(0)
    }
}
