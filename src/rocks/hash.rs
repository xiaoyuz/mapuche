use crate::config::{
    async_del_hash_threshold_or_default, async_expire_hash_threshold_or_default,
    config_meta_key_number_or_default,
};
use crate::db::DBInner;
use crate::rocks::client::{get_version_for_new, RocksClient};
use crate::rocks::encoding::{DataType, KeyDecoder};
use crate::rocks::errors::{REDIS_VALUE_IS_NOT_INTEGER_ERR, REDIS_WRONG_TYPE_ERR};
use crate::rocks::kv::bound_range::BoundRange;
use crate::rocks::kv::key::Key;
use crate::rocks::kv::kvpair::KvPair;
use crate::rocks::kv::value::Value;
use crate::rocks::transaction::RocksTransaction;
use crate::rocks::{
    Result as RocksResult, TxnCommand, CF_NAME_GC, CF_NAME_GC_VERSION, CF_NAME_HASH_DATA,
    CF_NAME_HASH_SUB_META, CF_NAME_META,
};
use crate::utils::{
    count_unique_keys, key_is_expired, resp_array, resp_bulk, resp_err, resp_int, resp_nil, resp_ok,
};
use crate::Frame;
use rocksdb::ColumnFamilyRef;

use std::collections::HashMap;
use std::ops::Range;

pub struct HashCF<'a> {
    meta_cf: ColumnFamilyRef<'a>,
    sub_meta_cf: ColumnFamilyRef<'a>,
    gc_cf: ColumnFamilyRef<'a>,
    gc_version_cf: ColumnFamilyRef<'a>,
    data_cf: ColumnFamilyRef<'a>,
}

impl<'a> HashCF<'a> {
    pub fn new(client: &'a RocksClient) -> Self {
        HashCF {
            meta_cf: client.cf_handle(CF_NAME_META).unwrap(),
            sub_meta_cf: client.cf_handle(CF_NAME_HASH_SUB_META).unwrap(),
            gc_cf: client.cf_handle(CF_NAME_GC).unwrap(),
            gc_version_cf: client.cf_handle(CF_NAME_GC_VERSION).unwrap(),
            data_cf: client.cf_handle(CF_NAME_HASH_DATA).unwrap(),
        }
    }
}

pub struct HashCommand<'a> {
    inner_db: &'a DBInner,
}

impl<'a> HashCommand<'a> {
    pub fn new(inner_db: &'a DBInner) -> Self {
        Self { inner_db }
    }

    pub async fn hset(
        self,
        key: &str,
        fvs: &[KvPair],
        is_hmset: bool,
        is_nx: bool,
    ) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = HashCF::new(client);
        let key = key.to_owned();
        let fvs_copy = fvs.to_vec();
        let fvs_len = fvs_copy.len();
        let idx = self.inner_db.gen_next_meta_index();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);

        let resp = client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type is hash
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    // already exists
                    let (ttl, mut version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);

                    // gerate a random index, update sub meta key, create a new sub meta key with this index
                    let sub_meta_key = self
                        .inner_db
                        .key_encoder
                        .encode_sub_meta_key(&key, version, idx);
                    // create or update it
                    let sub_meta_value_res =
                        txn.get_for_update(cfs.sub_meta_cf.clone(), sub_meta_key.clone())?;

                    let mut expired = false;

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
                    } else if is_nx {
                        // when is_nx == true, fvs_len must be 1
                        let kv = fvs_copy.get(0).unwrap();
                        let field: Vec<u8> = kv.clone().0.into();
                        let data_key = self.inner_db.key_encoder.encode_hash_data_key(
                            &key,
                            &String::from_utf8_lossy(&field),
                            version,
                        );
                        if txn.get(cfs.data_cf.clone(), data_key)?.is_some() {
                            return Ok(0);
                        }
                    }

                    let mut added_count = 1;
                    if !is_nx {
                        let mut fields_data_key = Vec::with_capacity(fvs_len);
                        for kv in fvs_copy.clone() {
                            let field: Vec<u8> = kv.0.into();
                            let datakey = self.inner_db.key_encoder.encode_hash_data_key(
                                &key,
                                &String::from_utf8_lossy(&field),
                                version,
                            );
                            fields_data_key.push(datakey);
                        }
                        // batch get
                        let real_fields_count = count_unique_keys(&fields_data_key);
                        added_count = real_fields_count as i64
                            - txn
                                .batch_get_for_update(cfs.data_cf.clone(), fields_data_key)?
                                .len() as i64;
                    }

                    for kv in fvs_copy {
                        let field: Vec<u8> = kv.0.into();
                        let data_key = self.inner_db.key_encoder.encode_hash_data_key(
                            &key,
                            &String::from_utf8_lossy(&field),
                            version,
                        );
                        txn.put(cfs.data_cf.clone(), data_key, kv.1)?;
                    }

                    let new_sub_meta_value = sub_meta_value_res.map_or_else(
                        || added_count.to_be_bytes().to_vec(),
                        |sub_meta_value| {
                            let sub_size = i64::from_be_bytes(sub_meta_value.try_into().unwrap());

                            (sub_size + added_count).to_be_bytes().to_vec()
                        },
                    );
                    txn.put(cfs.sub_meta_cf.clone(), sub_meta_key, new_sub_meta_value)?;
                    if expired {
                        // add meta key
                        let meta_size = config_meta_key_number_or_default();
                        let new_metaval = self
                            .inner_db
                            .key_encoder
                            .encode_hash_meta_value(ttl, version, meta_size);
                        txn.put(cfs.meta_cf.clone(), meta_key, new_metaval)?;
                    }
                }
                None => {
                    let version = get_version_for_new(
                        txn,
                        cfs.gc_cf.clone(),
                        cfs.gc_version_cf.clone(),
                        &key,
                        &self.inner_db.key_encoder,
                    )?;

                    // set sub meta key with a random index
                    let sub_meta_key = self
                        .inner_db
                        .key_encoder
                        .encode_sub_meta_key(&key, version, idx);
                    // lock sub meta key
                    txn.get_for_update(cfs.sub_meta_cf.clone(), sub_meta_key.clone())?;

                    // not exists
                    let ttl = 0;
                    let mut fields_data_key = vec![];
                    for kv in fvs_copy.clone() {
                        let field: Vec<u8> = kv.0.into();
                        let datakey = self.inner_db.key_encoder.encode_hash_data_key(
                            &key,
                            &String::from_utf8_lossy(&field),
                            version,
                        );
                        fields_data_key.push(datakey);
                    }
                    let real_fields_count = count_unique_keys(&fields_data_key);

                    for kv in fvs_copy {
                        let field: Vec<u8> = kv.0.into();
                        let datakey = self.inner_db.key_encoder.encode_hash_data_key(
                            &key,
                            &String::from_utf8_lossy(&field),
                            version,
                        );
                        txn.put(cfs.data_cf.clone(), datakey, kv.1)?;
                    }

                    // set meta key
                    let meta_size = config_meta_key_number_or_default();
                    let new_metaval = self
                        .inner_db
                        .key_encoder
                        .encode_hash_meta_value(ttl, version, meta_size);
                    txn.put(cfs.meta_cf.clone(), meta_key, new_metaval)?;

                    txn.put(
                        cfs.sub_meta_cf.clone(),
                        sub_meta_key,
                        real_fields_count.to_be_bytes().to_vec(),
                    )?;
                }
            }
            Ok(fvs_len)
        });

        match resp {
            Ok(num) => {
                if is_hmset {
                    Ok(resp_ok())
                } else {
                    Ok(resp_int(num as i64))
                }
            }
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn hget(self, key: &str, field: &str) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = HashCF::new(client);
        let key = key.to_owned();
        let field = field.to_owned();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type is hash
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);

                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(resp_nil());
                    }

                    let data_key = self
                        .inner_db
                        .key_encoder
                        .encode_hash_data_key(&key, &field, version);

                    txn.get(cfs.data_cf.clone(), data_key)?
                        .map_or_else(|| Ok(resp_nil()), |data| Ok(resp_bulk(data)))
                }
                None => Ok(resp_nil()),
            }
        })
    }

    pub async fn hstrlen(self, key: &str, field: &str) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = HashCF::new(client);
        let key = key.to_owned();
        let field = field.to_owned();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type is hash
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);

                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(resp_int(0));
                    }

                    let data_key = self
                        .inner_db
                        .key_encoder
                        .encode_hash_data_key(&key, &field, version);

                    txn.get(cfs.data_cf.clone(), data_key)?
                        .map_or_else(|| Ok(resp_int(0)), |data| Ok(resp_int(data.len() as i64)))
                }
                None => Ok(resp_int(0)),
            }
        })
    }

    pub async fn hexists(self, key: &str, field: &str) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = HashCF::new(client);
        let key = key.to_owned();
        let field = field.to_owned();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type is hash
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);

                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(resp_int(0));
                    }

                    let data_key = self
                        .inner_db
                        .key_encoder
                        .encode_hash_data_key(&key, &field, version);

                    if txn.get(cfs.data_cf.clone(), data_key)?.is_some() {
                        Ok(resp_int(1))
                    } else {
                        Ok(resp_int(0))
                    }
                }
                None => Ok(resp_int(0)),
            }
        })
    }

    pub async fn hmget(self, key: &str, fields: &[String]) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = HashCF::new(client);
        let key = key.to_owned();
        let fields = fields.to_owned();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);

        let mut resp = Vec::with_capacity(fields.len());

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type is hash
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);

                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(resp_array(vec![]));
                    }

                    let mut field_data_keys = Vec::with_capacity(fields.len());
                    for field in &fields {
                        let data_key = self
                            .inner_db
                            .key_encoder
                            .encode_hash_data_key(&key, field, version);
                        field_data_keys.push(data_key);
                    }

                    // batch get
                    let fields_result = txn
                        .batch_get(cfs.data_cf.clone(), field_data_keys)?
                        .into_iter()
                        .map(|kv| kv.into())
                        .collect::<HashMap<Key, Value>>();

                    for field in &fields {
                        let data_key = self
                            .inner_db
                            .key_encoder
                            .encode_hash_data_key(&key, field, version);
                        match fields_result.get(&data_key) {
                            Some(data) => resp.push(resp_bulk(data.to_vec())),
                            None => resp.push(resp_nil()),
                        }
                    }
                }
                None => {
                    for _ in 0..fields.len() {
                        resp.push(resp_nil());
                    }
                }
            }
            Ok(resp_array(resp))
        })
    }

    pub async fn hlen(self, key: &str) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = HashCF::new(client);
        let key = key.to_owned();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type is hash
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);

                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(resp_int(0));
                    }

                    let meta_size = self.sum_key_size(&key, version)?;
                    Ok(resp_int(meta_size))
                }
                None => Ok(resp_int(0)),
            }
        })
    }

    pub async fn hgetall(
        self,
        key: &str,
        with_field: bool,
        with_value: bool,
    ) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = HashCF::new(client);
        let key = key.to_owned();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type is hash
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);

                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(resp_nil());
                    }

                    let range: Range<Key> = self
                        .inner_db
                        .key_encoder
                        .encode_hash_data_key_start(&key, version)
                        ..self
                            .inner_db
                            .key_encoder
                            .encode_hash_data_key_end(&key, version);
                    let bound_range: BoundRange = range.into();
                    // scan return iterator
                    let iter = txn.scan(cfs.data_cf.clone(), bound_range, u32::MAX)?;

                    let resp: Vec<Frame>;
                    if with_field && with_value {
                        resp = iter
                            .flat_map(|kv| {
                                let field: Vec<u8> =
                                    KeyDecoder::decode_key_hash_userkey_from_datakey(&key, kv.0);
                                [resp_bulk(field), resp_bulk(kv.1)]
                            })
                            .collect();
                    } else if with_field {
                        resp = iter
                            .flat_map(|kv| {
                                let field: Vec<u8> =
                                    KeyDecoder::decode_key_hash_userkey_from_datakey(&key, kv.0);
                                [resp_bulk(field)]
                            })
                            .collect();
                    } else {
                        resp = iter.flat_map(|kv| [resp_bulk(kv.1)]).collect();
                    }
                    Ok(resp_array(resp))
                }
                None => Ok(resp_array(vec![])),
            }
        })
    }

    pub async fn hdel(self, key: &str, fields: &[String]) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = HashCF::new(client);
        let key = key.to_owned();
        let fields = fields.to_vec();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);

        let resp = client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type is hash
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);

                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(0);
                    }

                    let mut deleted: i64 = 0;
                    let data_keys: Vec<Key> = fields
                        .iter()
                        .map(|field| {
                            self.inner_db
                                .key_encoder
                                .encode_hash_data_key(&key, field, version)
                        })
                        .collect();
                    for pair in txn.batch_get_for_update(cfs.data_cf.clone(), data_keys)? {
                        txn.del(cfs.data_cf.clone(), pair.0)?;
                        deleted += 1;
                    }

                    let idx = self.inner_db.gen_next_meta_index();

                    // txn lock will be called in txnkv_sum_key_size, so release txn lock first
                    let old_size = self.sum_key_size(&key, version)?;

                    // update sub meta key or clear all meta and sub meta key if needed
                    if old_size <= deleted {
                        txn.del(cfs.meta_cf.clone(), meta_key)?;
                        let bound_range = self
                            .inner_db
                            .key_encoder
                            .encode_sub_meta_key_range(&key, version);
                        let iter = txn.scan_keys(cfs.sub_meta_cf.clone(), bound_range, u32::MAX)?;
                        for k in iter {
                            txn.del(cfs.sub_meta_cf.clone(), k)?;
                        }
                    } else {
                        // set sub meta key with a random index
                        let sub_meta_key = self
                            .inner_db
                            .key_encoder
                            .encode_sub_meta_key(&key, version, idx);
                        // create it with negtive value if sub meta key not exists
                        let new_size = txn
                            .get_for_update(cfs.sub_meta_cf.clone(), sub_meta_key.clone())?
                            .map_or_else(
                                || -deleted,
                                |value| {
                                    let sub_size = i64::from_be_bytes(value.try_into().unwrap());
                                    sub_size - deleted
                                },
                            );
                        // new_size may be negtive
                        txn.put(
                            cfs.sub_meta_cf.clone(),
                            sub_meta_key,
                            new_size.to_be_bytes().to_vec(),
                        )?;
                    }
                    Ok(deleted)
                }
                None => Ok(0),
            }
        });
        match resp {
            Ok(n) => Ok(resp_int(n)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn hincrby(self, key: &str, field: &str, step: i64) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = HashCF::new(client);
        let key = key.to_owned();
        let field = field.to_owned();
        let idx = self.inner_db.gen_next_meta_index();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);

        let resp = client.exec_txn(|txn| {
            let prev_int;
            let data_key;
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type is hash
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let mut expired = false;
                    let (ttl, mut version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);

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

                    data_key = self
                        .inner_db
                        .key_encoder
                        .encode_hash_data_key(&key, &field, version);

                    match txn.get_for_update(cfs.data_cf.clone(), data_key.clone())? {
                        Some(data_value) => {
                            // try to convert to int
                            match String::from_utf8_lossy(&data_value).parse::<i64>() {
                                Ok(ival) => {
                                    prev_int = ival;
                                }
                                Err(_) => {
                                    return Err(REDIS_VALUE_IS_NOT_INTEGER_ERR);
                                }
                            }
                        }
                        None => {
                            // filed not exist
                            prev_int = 0;
                            // add size to a random sub meta key
                            let sub_meta_key = self
                                .inner_db
                                .key_encoder
                                .encode_sub_meta_key(&key, version, idx);

                            let sub_size = txn
                                .get_for_update(cfs.sub_meta_cf.clone(), sub_meta_key.clone())?
                                .map_or_else(
                                    || 1,
                                    |value| i64::from_be_bytes(value.try_into().unwrap()),
                                );

                            // add or update sub meta key
                            txn.put(
                                cfs.sub_meta_cf.clone(),
                                sub_meta_key,
                                sub_size.to_be_bytes().to_vec(),
                            )?;

                            // add meta key if needed
                            if expired {
                                // add meta key
                                let meta_size = config_meta_key_number_or_default();
                                let meta_value = self
                                    .inner_db
                                    .key_encoder
                                    .encode_hash_meta_value(ttl, version, meta_size);
                                txn.put(cfs.meta_cf.clone(), meta_key, meta_value)?;
                            }
                        }
                    }
                }
                None => {
                    let version = get_version_for_new(
                        txn,
                        cfs.gc_cf.clone(),
                        cfs.gc_version_cf.clone(),
                        &key,
                        &self.inner_db.key_encoder,
                    )?;

                    prev_int = 0;
                    // create new meta key first
                    let meta_size = config_meta_key_number_or_default();
                    let meta_value = self
                        .inner_db
                        .key_encoder
                        .encode_hash_meta_value(0, version, meta_size);
                    txn.put(cfs.meta_cf.clone(), meta_key, meta_value)?;

                    // add a sub meta key with a random index
                    let sub_meta_key = self
                        .inner_db
                        .key_encoder
                        .encode_sub_meta_key(&key, version, idx);
                    txn.put(
                        cfs.sub_meta_cf.clone(),
                        sub_meta_key,
                        1_i64.to_be_bytes().to_vec(),
                    )?;
                    data_key = self
                        .inner_db
                        .key_encoder
                        .encode_hash_data_key(&key, &field, version);
                }
            }
            let new_int = prev_int + step;
            // update data key
            txn.put(
                cfs.data_cf.clone(),
                data_key,
                new_int.to_string().as_bytes().to_vec(),
            )?;

            Ok(new_int)
        });
        match resp {
            Ok(n) => Ok(resp_int(n)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    fn sum_key_size(&self, key: &str, version: u16) -> RocksResult<i64> {
        let client = &self.inner_db.client;
        let cfs = HashCF::new(client);
        let key = key.to_owned();

        client.exec_txn(move |txn| {
            // check if meta key exists or already expired
            let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
            match txn.get(cfs.meta_cf, meta_key)? {
                Some(meta_value) => {
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
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

impl TxnCommand for HashCommand<'_> {
    fn txn_del(&self, txn: &RocksTransaction, key: &str) -> RocksResult<()> {
        let key = key.to_owned();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        let cfs = HashCF::new(&self.inner_db.client);

        match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
            Some(meta_value) => {
                let (_, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                let meta_size = self.sum_key_size(&key, version)?;

                if meta_size > async_del_hash_threshold_or_default() as i64 {
                    // do async del
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
                        vec![self.inner_db.key_encoder.get_type_bytes(DataType::Hash)],
                    )?;
                } else {
                    let bound_range = self
                        .inner_db
                        .key_encoder
                        .encode_hash_data_key_range(&key, version);
                    // scan return iterator
                    let iter = txn.scan_keys(cfs.data_cf.clone(), bound_range, u32::MAX)?;

                    for k in iter {
                        txn.del(cfs.data_cf.clone(), k)?;
                    }

                    let sub_meta_bound_range = self
                        .inner_db
                        .key_encoder
                        .encode_sub_meta_key_range(&key, version);
                    let sub_meta_iter =
                        txn.scan_keys(cfs.sub_meta_cf.clone(), sub_meta_bound_range, u32::MAX)?;
                    for k in sub_meta_iter {
                        txn.del(cfs.sub_meta_cf.clone(), k)?;
                    }

                    txn.del(cfs.meta_cf.clone(), meta_key)?;
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn txn_expire_if_needed(&self, txn: &RocksTransaction, key: &str) -> RocksResult<i64> {
        let key = key.to_owned();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        let cfs = HashCF::new(&self.inner_db.client);

        match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
            Some(meta_value) => {
                let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);
                if !key_is_expired(ttl) {
                    return Ok(0);
                }
                let meta_size = self.sum_key_size(&key, version)?;

                if meta_size > async_expire_hash_threshold_or_default() as i64 {
                    // do async del
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
                        vec![self.inner_db.key_encoder.get_type_bytes(DataType::Hash)],
                    )?;
                } else {
                    let bound_range = self
                        .inner_db
                        .key_encoder
                        .encode_hash_data_key_range(&key, version);
                    // scan return iterator
                    let iter = txn.scan_keys(cfs.data_cf.clone(), bound_range, u32::MAX)?;

                    for k in iter {
                        txn.del(cfs.data_cf.clone(), k)?;
                    }

                    let sub_meta_bound_range = self
                        .inner_db
                        .key_encoder
                        .encode_sub_meta_key_range(&key, version);
                    let sub_meta_iter =
                        txn.scan_keys(cfs.sub_meta_cf.clone(), sub_meta_bound_range, u32::MAX)?;
                    for k in sub_meta_iter {
                        txn.del(cfs.sub_meta_cf.clone(), k)?;
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
        let cfs = HashCF::new(&self.inner_db.client);
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
            .encode_hash_meta_value(timestamp, version, 0);
        txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;
        Ok(1)
    }

    fn txn_gc(&self, txn: &RocksTransaction, key: &str, version: u16) -> RocksResult<()> {
        let cfs = HashCF::new(&self.inner_db.client);
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
            .encode_hash_data_key_range(key, version);
        let iter = txn.scan_keys(cfs.data_cf.clone(), bound_range, u32::MAX)?;
        for k in iter {
            txn.del(cfs.data_cf.clone(), k)?;
        }
        Ok(())
    }
}
