use crate::config::{async_del_set_threshold_or_default, async_expire_set_threshold_or_default};
use crate::db::DBInner;
use crate::rocks::client::{get_version_for_new, RocksClient};
use crate::rocks::encoding::{DataType, KeyDecoder};
use crate::rocks::errors::REDIS_WRONG_TYPE_ERR;
use crate::rocks::kv::key::Key;
use crate::rocks::kv::value::Value;
use crate::rocks::transaction::RocksTransaction;
use crate::rocks::{
    Result as RocksResult, TxnCommand, CF_NAME_GC, CF_NAME_GC_VERSION, CF_NAME_META,
    CF_NAME_SET_DATA, CF_NAME_SET_SUB_META,
};
use crate::utils::{
    count_unique_keys, key_is_expired, resp_array, resp_bulk, resp_err, resp_int, resp_nil,
};
use crate::Frame;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rocksdb::ColumnFamilyRef;

use std::collections::HashMap;

const RANDOM_BASE: i64 = 100;

pub struct SetCF<'a> {
    meta_cf: ColumnFamilyRef<'a>,
    sub_meta_cf: ColumnFamilyRef<'a>,
    gc_cf: ColumnFamilyRef<'a>,
    gc_version_cf: ColumnFamilyRef<'a>,
    data_cf: ColumnFamilyRef<'a>,
}

impl<'a> SetCF<'a> {
    pub fn new(client: &'a RocksClient) -> Self {
        SetCF {
            meta_cf: client.cf_handle(CF_NAME_META).unwrap(),
            sub_meta_cf: client.cf_handle(CF_NAME_SET_SUB_META).unwrap(),
            gc_cf: client.cf_handle(CF_NAME_GC).unwrap(),
            gc_version_cf: client.cf_handle(CF_NAME_GC_VERSION).unwrap(),
            data_cf: client.cf_handle(CF_NAME_SET_DATA).unwrap(),
        }
    }
}

pub struct SetCommand<'a> {
    inner_db: &'a DBInner,
}

impl<'a> SetCommand<'a> {
    pub fn new(inner_db: &'a DBInner) -> Self {
        Self { inner_db }
    }

    pub async fn sadd(self, key: &str, members: &Vec<String>) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = SetCF::new(client);
        let key = key.to_owned();
        let members = members.to_owned();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        let rand_idx = self.inner_db.gen_next_meta_index();

        let resp = client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let mut expired = false;
                    let (ttl, mut version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);

                    // choose a random sub meta key for update, create if not exists
                    let sub_meta_key = self
                        .inner_db
                        .key_encoder
                        .encode_sub_meta_key(&key, version, rand_idx);
                    let sub_meta_value_res =
                        txn.get_for_update(cfs.sub_meta_cf.clone(), sub_meta_key.clone())?;

                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        expired = true;
                        version = get_version_for_new(
                            txn,
                            cfs.gc_cf.clone(),
                            cfs.gc_version_cf.clone(),
                            &key,
                            &self.inner_db.key_encoder,
                        )?;
                    }
                    let mut member_data_keys = Vec::with_capacity(members.len());
                    for m in &members {
                        let data_key = self
                            .inner_db
                            .key_encoder
                            .encode_set_data_key(&key, m, version);
                        member_data_keys.push(data_key);
                    }
                    // batch get
                    // count the unique members
                    let real_member_count = count_unique_keys(&member_data_keys);
                    let values_count = txn
                        .batch_get_for_update(cfs.data_cf.clone(), member_data_keys)?
                        .len();
                    let added = real_member_count as i64 - values_count as i64;
                    for m in &members {
                        let data_key = self
                            .inner_db
                            .key_encoder
                            .encode_set_data_key(&key, m, version);
                        txn.put(cfs.data_cf.clone(), data_key, vec![0])?;
                    }

                    let new_sub_meta_value = sub_meta_value_res.map_or_else(
                        || added,
                        |value| {
                            let old_sub_meta_value = i64::from_be_bytes(value.try_into().unwrap());
                            old_sub_meta_value + added
                        },
                    );
                    txn.put(
                        cfs.sub_meta_cf.clone(),
                        sub_meta_key,
                        new_sub_meta_value.to_be_bytes().to_vec(),
                    )?;

                    // create a new meta key if key already expired above
                    if expired {
                        let new_meta_value = self
                            .inner_db
                            .key_encoder
                            .encode_set_meta_value(0, version, 0);
                        txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;
                    }

                    Ok(added)
                }
                None => {
                    let version = get_version_for_new(
                        txn,
                        cfs.gc_cf.clone(),
                        cfs.gc_version_cf.clone(),
                        &key,
                        &self.inner_db.key_encoder,
                    )?;
                    // create sub meta key with a random index
                    let sub_meta_key = self
                        .inner_db
                        .key_encoder
                        .encode_sub_meta_key(&key, version, rand_idx);
                    // lock sub meta key
                    txn.get_for_update(cfs.sub_meta_cf.clone(), sub_meta_key.clone())?;

                    // create new meta key and meta value
                    for m in &members {
                        // check member already exists
                        let data_key = self
                            .inner_db
                            .key_encoder
                            .encode_set_data_key(&key, m, version);
                        // value can not be vec![] if use cse as backend
                        txn.put(cfs.data_cf.clone(), data_key, vec![0])?;
                    }
                    // create meta key
                    let meta_value = self
                        .inner_db
                        .key_encoder
                        .encode_set_meta_value(0, version, 0);
                    txn.put(cfs.meta_cf.clone(), meta_key, meta_value)?;

                    let added = count_unique_keys(&members) as i64;

                    txn.put(
                        cfs.sub_meta_cf.clone(),
                        sub_meta_key,
                        added.to_be_bytes().to_vec(),
                    )?;
                    Ok(added)
                }
            }
        });

        match resp {
            Ok(v) => Ok(resp_int(v)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn scard(self, key: &str) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = SetCF::new(client);
        let meta_key = self.inner_db.key_encoder.encode_meta_key(key);
        let key = key.to_owned();

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key)? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                        return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                    }

                    let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(resp_int(0));
                    }

                    let size = self.sum_key_size(&key, version)?;
                    Ok(resp_int(size))
                }
                None => Ok(resp_int(0)),
            }
        })
    }

    pub async fn sismember(
        self,
        key: &str,
        members: &Vec<String>,
        resp_in_arr: bool,
    ) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = SetCF::new(client);
        let member_len = members.len();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(key);
        let key = key.to_owned();
        let members = members.to_owned();

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key)? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                        return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                    }

                    let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        if !resp_in_arr {
                            return Ok(resp_int(0));
                        } else {
                            return Ok(resp_array(vec![resp_int(0); member_len]));
                        }
                    }
                    if !resp_in_arr {
                        let data_key = self.inner_db.key_encoder.encode_set_data_key(
                            &key,
                            &members[0],
                            version,
                        );
                        if txn.get(cfs.data_cf.clone(), data_key)?.is_some() {
                            Ok(resp_int(1))
                        } else {
                            Ok(resp_int(0))
                        }
                    } else {
                        let mut resp = vec![];
                        let mut member_data_keys = Vec::with_capacity(members.len());
                        for m in &members {
                            let data_key = self
                                .inner_db
                                .key_encoder
                                .encode_set_data_key(&key, m, version);
                            member_data_keys.push(data_key);
                        }

                        // batch get
                        let member_result: HashMap<Key, Value> = txn
                            .batch_get(cfs.data_cf.clone(), member_data_keys)?
                            .into_iter()
                            .map(|kv| (kv.0, kv.1))
                            .collect();

                        for m in &members {
                            let data_key = self
                                .inner_db
                                .key_encoder
                                .encode_set_data_key(&key, m, version);
                            match member_result.get(&data_key) {
                                Some(_) => resp.push(resp_int(1)),
                                None => resp.push(resp_int(0)),
                            }
                        }

                        Ok(resp_array(resp))
                    }
                }
                None => {
                    if !resp_in_arr {
                        Ok(resp_int(0))
                    } else {
                        Ok(resp_array(vec![resp_int(0); member_len]))
                    }
                }
            }
        })
    }

    pub async fn srandmemeber(
        self,
        key: &str,
        count: i64,
        repeatable: bool,
        array_resp: bool,
    ) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = SetCF::new(client);
        let meta_key = self.inner_db.key_encoder.encode_meta_key(key);
        let key = key.to_owned();

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key)? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                        return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                    }

                    let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(resp_array(vec![]));
                    }

                    // create random
                    let mut rng = SmallRng::from_entropy();
                    let mut ele_count = RANDOM_BASE;
                    if count > RANDOM_BASE {
                        ele_count = count;
                    }

                    let bound_range = self
                        .inner_db
                        .key_encoder
                        .encode_set_data_key_range(&key, version);
                    let iter = txn.scan_keys(
                        cfs.data_cf.clone(),
                        bound_range,
                        ele_count.try_into().unwrap(),
                    )?;
                    let mut resp: Vec<Frame> = iter
                        .map(|k| {
                            // decode member from data key
                            let user_key = KeyDecoder::decode_key_set_member_from_datakey(&key, k);
                            resp_bulk(user_key)
                        })
                        .collect();

                    // shuffle the resp vector
                    resp.shuffle(&mut rng);

                    let resp_len = resp.len();
                    if !array_resp {
                        // called with no count argument, return bulk reply
                        // choose a random from resp
                        let rand_idx = rng.gen_range(0..resp_len);
                        return Ok(resp[rand_idx].clone());
                    }

                    // check resp is enough when repeatable is set, fill it with random element in resp vector
                    while repeatable && (resp.len() as i64) < count {
                        let rand_idx = rng.gen_range(0..resp_len);
                        resp.push(resp[rand_idx].clone());
                    }

                    // if count is less than resp.len(), truncate it
                    if count < resp_len as i64 {
                        resp.truncate(count.try_into().unwrap());
                    }

                    Ok(resp_array(resp))
                }
                None => {
                    if array_resp {
                        Ok(resp_array(vec![]))
                    } else {
                        Ok(resp_nil())
                    }
                }
            }
        })
    }

    pub async fn smembers(self, key: &str) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = SetCF::new(client);
        let meta_key = self.inner_db.key_encoder.encode_meta_key(key);
        let key = key.to_owned();

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key)? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                        return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                    }

                    let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(resp_array(vec![]));
                    }

                    let bound_range = self
                        .inner_db
                        .key_encoder
                        .encode_set_data_key_range(&key, version);
                    let iter = txn.scan_keys(cfs.data_cf.clone(), bound_range, u32::MAX)?;

                    let resp = iter
                        .map(|k| {
                            // decode member from data key
                            let user_key = KeyDecoder::decode_key_set_member_from_datakey(&key, k);
                            resp_bulk(user_key)
                        })
                        .collect();
                    Ok(resp_array(resp))
                }
                None => Ok(resp_array(vec![])),
            }
        })
    }

    pub async fn srem(self, key: &str, members: &Vec<String>) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = SetCF::new(client);
        let meta_key = self.inner_db.key_encoder.encode_meta_key(key);
        let key = key.to_owned();
        let members = members.to_owned();
        let rand_idx = self.inner_db.gen_next_meta_index();

        let resp = client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);

                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(0);
                    }

                    let size = self.sum_key_size(&key, version)?;
                    let data_keys: Vec<Key> = members
                        .iter()
                        .map(|member| {
                            self.inner_db
                                .key_encoder
                                .encode_set_data_key(&key, member, version)
                        })
                        .collect();
                    let mut removed: i64 = 0;

                    for pair in txn.batch_get_for_update(cfs.data_cf.clone(), data_keys)? {
                        txn.del(cfs.data_cf.clone(), pair.0)?;
                        removed += 1;
                    }

                    // check if all items cleared, delete meta key and all sub meta keys if needed
                    if removed >= size {
                        txn.del(cfs.meta_cf, meta_key)?;
                        let meta_bound_range = self
                            .inner_db
                            .key_encoder
                            .encode_sub_meta_key_range(&key, version);
                        let iter =
                            txn.scan_keys(cfs.sub_meta_cf.clone(), meta_bound_range, u32::MAX)?;
                        for k in iter {
                            txn.del(cfs.sub_meta_cf.clone(), k)?;
                        }
                    } else {
                        // choose a random sub meta key, update it
                        let sub_meta_key = self
                            .inner_db
                            .key_encoder
                            .encode_sub_meta_key(&key, version, rand_idx);
                        let new_sub_meta_value = txn
                            .get_for_update(cfs.sub_meta_cf.clone(), sub_meta_key.clone())?
                            .map_or_else(
                                || -removed,
                                |v| {
                                    let old_sub_meta_value =
                                        i64::from_be_bytes(v.try_into().unwrap());
                                    old_sub_meta_value - removed
                                },
                            );
                        txn.put(
                            cfs.sub_meta_cf.clone(),
                            sub_meta_key,
                            new_sub_meta_value.to_be_bytes().to_vec(),
                        )?;
                    }

                    Ok(removed)
                }
                None => Ok(0),
            }
        });
        match resp {
            Ok(v) => Ok(resp_int(v)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    /// spop will pop members by alphabetical order
    pub async fn spop(self, key: &str, count: u64) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = SetCF::new(client);
        let meta_key = self.inner_db.key_encoder.encode_meta_key(key);
        let key = key.to_owned();
        let rand_idx = self.inner_db.gen_next_meta_index();

        let resp = client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(vec![]);
                    }

                    let bound_range = self
                        .inner_db
                        .key_encoder
                        .encode_set_data_key_range(&key, version);
                    let iter =
                        txn.scan_keys(cfs.data_cf.clone(), bound_range, count.try_into().unwrap())?;

                    let mut data_key_to_delete = vec![];
                    let resp = iter
                        .map(|k| {
                            data_key_to_delete.push(k.clone());
                            // decode member from data key
                            let member = KeyDecoder::decode_key_set_member_from_datakey(&key, k);
                            resp_bulk(member)
                        })
                        .collect();

                    let poped_count = data_key_to_delete.len() as i64;
                    for k in data_key_to_delete {
                        txn.del(cfs.data_cf.clone(), k)?;
                    }

                    // txn will be lock inner txnkv_sum_key_size, so release it first
                    let size = self.sum_key_size(&key, version)?;

                    // update or delete meta key
                    if poped_count >= size {
                        // delete meta key
                        txn.del(cfs.meta_cf, meta_key)?;
                        // delete all sub meta keys
                        let meta_bound_range = self
                            .inner_db
                            .key_encoder
                            .encode_sub_meta_key_range(&key, version);
                        let iter =
                            txn.scan_keys(cfs.sub_meta_cf.clone(), meta_bound_range, u32::MAX)?;
                        for k in iter {
                            txn.del(cfs.sub_meta_cf.clone(), k)?;
                        }
                    } else {
                        // update random meta key
                        let sub_meta_key = self
                            .inner_db
                            .key_encoder
                            .encode_sub_meta_key(&key, version, rand_idx);
                        let new_sub_meta_value = txn
                            .get_for_update(cfs.sub_meta_cf.clone(), sub_meta_key.clone())?
                            .map_or_else(
                                || -poped_count,
                                |v| {
                                    let old_sub_meta_value =
                                        i64::from_be_bytes(v.try_into().unwrap());
                                    old_sub_meta_value - poped_count
                                },
                            );
                        txn.put(
                            cfs.sub_meta_cf.clone(),
                            sub_meta_key,
                            new_sub_meta_value.to_be_bytes().to_vec(),
                        )?;
                    }
                    Ok(resp)
                }
                None => Ok(vec![]),
            }
        });
        match resp {
            Ok(mut v) => {
                if count == 1 {
                    if v.is_empty() {
                        Ok(resp_nil())
                    } else {
                        Ok(v.pop().unwrap())
                    }
                } else {
                    Ok(resp_array(v))
                }
            }
            Err(e) => Ok(resp_err(e)),
        }
    }

    fn sum_key_size(&self, key: &str, version: u16) -> RocksResult<i64> {
        let client = &self.inner_db.client;
        let cfs = SetCF::new(client);
        let key = key.to_owned();

        client.exec_txn(move |txn| {
            // check if meta key exists or already expired
            let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
            match txn.get(cfs.meta_cf, meta_key)? {
                Some(meta_value) => {
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let bound_range = self
                        .inner_db
                        .key_encoder
                        .encode_sub_meta_key_range(&key, version);
                    let iter = txn.scan(cfs.sub_meta_cf.clone(), bound_range, u32::MAX)?;
                    let sum = iter
                        .map(|kv| i64::from_be_bytes(kv.1.try_into().unwrap()))
                        .sum();
                    Ok(sum)
                }
                None => Ok(0),
            }
        })
    }
}

impl TxnCommand for SetCommand<'_> {
    fn txn_del(&self, txn: &RocksTransaction, key: &str) -> RocksResult<()> {
        let key = key.to_owned();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        let cfs = SetCF::new(&self.inner_db.client);

        match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
            Some(meta_value) => {
                let version = KeyDecoder::decode_key_version(&meta_value);
                let size = self.sum_key_size(&key, version)?;

                if size > async_del_set_threshold_or_default() as i64 {
                    // async del set
                    txn.del(cfs.meta_cf.clone(), meta_key)?;

                    let gc_key = self.inner_db.key_encoder.encode_gc_key(&key);
                    txn.put(cfs.gc_cf.clone(), gc_key, version.to_be_bytes().to_vec())?;

                    let gc_version_key = self
                        .inner_db
                        .key_encoder
                        .encode_gc_version_key(&key, version);
                    txn.put(
                        cfs.gc_version_cf.clone(),
                        gc_version_key,
                        vec![self.inner_db.key_encoder.get_type_bytes(DataType::Set)],
                    )?;
                } else {
                    let sub_meta_range = self
                        .inner_db
                        .key_encoder
                        .encode_sub_meta_key_range(&key, version);
                    let iter = txn.scan_keys(cfs.sub_meta_cf.clone(), sub_meta_range, u32::MAX)?;
                    for k in iter {
                        txn.del(cfs.sub_meta_cf.clone(), k)?;
                    }

                    let data_bound_range = self
                        .inner_db
                        .key_encoder
                        .encode_set_data_key_range(&key, version);
                    let iter = txn.scan_keys(cfs.data_cf.clone(), data_bound_range, u32::MAX)?;
                    for k in iter {
                        txn.del(cfs.data_cf.clone(), k)?;
                    }

                    txn.del(cfs.meta_cf, meta_key)?;
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn txn_expire_if_needed(&self, txn: &RocksTransaction, key: &str) -> RocksResult<i64> {
        let key = key.to_owned();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        let cfs = SetCF::new(&self.inner_db.client);

        match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
            Some(meta_value) => {
                let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                if !key_is_expired(ttl) {
                    return Ok(0);
                }
                let size = self.sum_key_size(&key, version)?;
                if size > async_expire_set_threshold_or_default() as i64 {
                    // async del set
                    txn.del(cfs.meta_cf.clone(), meta_key)?;

                    let gc_key = self.inner_db.key_encoder.encode_gc_key(&key);
                    txn.put(cfs.gc_cf.clone(), gc_key, version.to_be_bytes().to_vec())?;

                    let gc_version_key = self
                        .inner_db
                        .key_encoder
                        .encode_gc_version_key(&key, version);
                    txn.put(
                        cfs.gc_version_cf.clone(),
                        gc_version_key,
                        vec![self.inner_db.key_encoder.get_type_bytes(DataType::Set)],
                    )?;
                } else {
                    let sub_meta_range = self
                        .inner_db
                        .key_encoder
                        .encode_sub_meta_key_range(&key, version);

                    let iter = txn.scan_keys(cfs.sub_meta_cf.clone(), sub_meta_range, u32::MAX)?;
                    for k in iter {
                        txn.del(cfs.sub_meta_cf.clone(), k)?;
                    }

                    let data_bound_range = self
                        .inner_db
                        .key_encoder
                        .encode_set_data_key_range(&key, version);
                    let iter = txn.scan_keys(cfs.data_cf.clone(), data_bound_range, u32::MAX)?;
                    for k in iter {
                        txn.del(cfs.data_cf.clone(), k)?;
                    }

                    txn.del(cfs.meta_cf.clone(), meta_key)?;
                }
                Ok(1)
            }
            None => Ok(0),
        }
    }

    fn txn_expire(
        &self,
        txn: &RocksTransaction,
        key: &str,
        timestamp: i64,
        meta_value: &Value,
    ) -> RocksResult<i64> {
        let cfs = SetCF::new(&self.inner_db.client);
        let meta_key = self.inner_db.key_encoder.encode_meta_key(key);
        let ttl = KeyDecoder::decode_key_ttl(meta_value);
        if key_is_expired(ttl) {
            self.txn_expire_if_needed(txn, key)?;
            return Ok(0);
        }
        let version = KeyDecoder::decode_key_version(meta_value);
        let new_meta_value = self
            .inner_db
            .key_encoder
            .encode_set_meta_value(timestamp, version, 0);
        txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;
        Ok(1)
    }

    fn txn_gc(&self, txn: &RocksTransaction, key: &str, version: u16) -> RocksResult<()> {
        let cfs = SetCF::new(&self.inner_db.client);
        // delete all sub meta key of this key and version
        let bound_range = self
            .inner_db
            .key_encoder
            .encode_sub_meta_key_range(key, version);
        let iter = txn.scan_keys(cfs.sub_meta_cf.clone(), bound_range, u32::MAX)?;
        for k in iter {
            txn.del(cfs.sub_meta_cf.clone(), k)?;
        }
        // delete all data key of this key and version
        let bound_range = self
            .inner_db
            .key_encoder
            .encode_set_data_key_range(key, version);
        let iter = txn.scan_keys(cfs.data_cf.clone(), bound_range, u32::MAX)?;
        for k in iter {
            txn.del(cfs.data_cf.clone(), k)?;
        }
        Ok(())
    }
}
