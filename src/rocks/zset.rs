use crate::config::{async_del_zset_threshold_or_default, async_expire_zset_threshold_or_default};
use crate::metrics::REMOVED_EXPIRED_KEY_COUNTER;
use crate::rocks::client::{get_version_for_new, RocksRawClient};
use crate::rocks::encoding::{DataType, KeyDecoder};
use crate::rocks::errors::REDIS_WRONG_TYPE_ERR;
use crate::rocks::kv::bound_range::BoundRange;
use crate::rocks::kv::key::Key;
use crate::rocks::kv::value::Value;
use crate::rocks::transaction::RocksTransaction;
use crate::rocks::{
    gen_next_meta_index, get_client, Result as RocksResult, RocksCommand, CF_NAME_GC,
    CF_NAME_GC_VERSION, CF_NAME_META, CF_NAME_ZSET_DATA, CF_NAME_ZSET_SCORE, CF_NAME_ZSET_SUB_META,
    KEY_ENCODER,
};
use crate::utils::{key_is_expired, resp_array, resp_bulk, resp_err, resp_int, resp_nil};
use crate::Frame;
use rocksdb::ColumnFamilyRef;
use std::collections::HashMap;

pub struct ZsetCF<'a> {
    meta_cf: ColumnFamilyRef<'a>,
    sub_meta_cf: ColumnFamilyRef<'a>,
    gc_cf: ColumnFamilyRef<'a>,
    gc_version_cf: ColumnFamilyRef<'a>,
    data_cf: ColumnFamilyRef<'a>,
    score_cf: ColumnFamilyRef<'a>,
}

impl<'a> ZsetCF<'a> {
    pub fn new(client: &'a RocksRawClient) -> Self {
        ZsetCF {
            meta_cf: client.cf_handle(CF_NAME_META).unwrap(),
            sub_meta_cf: client.cf_handle(CF_NAME_ZSET_SUB_META).unwrap(),
            gc_cf: client.cf_handle(CF_NAME_GC).unwrap(),
            gc_version_cf: client.cf_handle(CF_NAME_GC_VERSION).unwrap(),
            data_cf: client.cf_handle(CF_NAME_ZSET_DATA).unwrap(),
            score_cf: client.cf_handle(CF_NAME_ZSET_SCORE).unwrap(),
        }
    }
}

#[derive(Clone)]
pub struct ZsetCommand;

impl ZsetCommand {
    pub async fn zadd(
        self,
        key: &str,
        members: &Vec<String>,
        scores: &Vec<f64>,
        exists: Option<bool>,
        changed_only: bool,
        _incr: bool,
    ) -> RocksResult<Frame> {
        let client = get_client();
        let cfs = ZsetCF::new(&client);
        let key = key.to_owned();
        let members = members.to_owned();
        let scores = scores.to_owned();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);
        let rand_idx = gen_next_meta_index();

        let resp = client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, mut version, _) = KeyDecoder::decode_key_meta(&meta_value);
                    let mut expired = false;
                    if key_is_expired(ttl) {
                        self.clone().txn_expire_if_needed(txn, &client, &key)?;
                        expired = true;
                        version = get_version_for_new(
                            txn,
                            cfs.gc_cf.clone(),
                            cfs.gc_version_cf.clone(),
                            &key,
                        )?;
                    }

                    let mut updated_count = 0;
                    let mut added_count = 0;

                    let data_keys: Vec<Key> = members
                        .iter()
                        .map(|member| {
                            KEY_ENCODER.encode_txn_kv_zset_data_key(&key, member, version)
                        })
                        .collect();
                    let data_map: HashMap<Key, Value> = txn
                        .batch_get(cfs.data_cf.clone(), data_keys)?
                        .into_iter()
                        .map(|pair| (pair.0, pair.1))
                        .collect();

                    for idx in 0..members.len() {
                        let data_key =
                            KEY_ENCODER.encode_txn_kv_zset_data_key(&key, &members[idx], version);
                        let new_score = scores[idx];
                        let score_key = KEY_ENCODER.encode_txn_kv_zset_score_key(
                            &key,
                            new_score,
                            &members[idx],
                            version,
                        );
                        let mut member_exists = false;
                        let old_data_value = data_map.get(&data_key);
                        let mut old_data_value_data: Vec<u8> = vec![];
                        if let Some(v) = old_data_value {
                            member_exists = true;
                            old_data_value_data = v.clone();
                        }

                        if let Some(v) = exists {
                            // NX|XX
                            if (v && member_exists) || (!v && !member_exists) {
                                if !member_exists {
                                    added_count += 1;
                                }
                                // XX Only update elements that already exists
                                // NX Only add elements that not exists
                                if changed_only {
                                    if !member_exists {
                                        updated_count += 1;
                                    } else {
                                        // check if score updated
                                        let old_score = KeyDecoder::decode_key_zset_data_value(
                                            &old_data_value_data,
                                        );
                                        if old_score != new_score {
                                            updated_count += 1;
                                        }
                                    }
                                }
                                let data_value =
                                    KEY_ENCODER.encode_txn_kv_zset_data_value(new_score);
                                txn.put(cfs.data_cf.clone(), data_key, data_value)?;

                                // delete old score key if exists
                                if member_exists {
                                    let old_score = KeyDecoder::decode_key_zset_data_value(
                                        &old_data_value_data,
                                    );
                                    if old_score != new_score {
                                        let old_score_key = KEY_ENCODER
                                            .encode_txn_kv_zset_score_key(
                                                &key,
                                                old_score,
                                                &members[idx],
                                                version,
                                            );
                                        txn.del(cfs.score_cf.clone(), old_score_key)?;
                                    }
                                }
                                txn.put(cfs.score_cf.clone(), score_key, members[idx].clone())?;
                            }
                        } else {
                            if !member_exists {
                                added_count += 1;
                            }
                            // no NX|XX argument
                            if changed_only {
                                if !member_exists {
                                    updated_count += 1;
                                } else {
                                    // check if score updated
                                    let old_score = KeyDecoder::decode_key_zset_data_value(
                                        &old_data_value_data,
                                    );
                                    if old_score != new_score {
                                        updated_count += 1;
                                    }
                                }
                            }
                            let data_value = KEY_ENCODER.encode_txn_kv_zset_data_value(new_score);
                            let member = members[idx].clone();
                            txn.put(cfs.data_cf.clone(), data_key, data_value)?;

                            // delete old score key if it exists
                            if member_exists {
                                let old_score =
                                    KeyDecoder::decode_key_zset_data_value(&old_data_value_data);
                                if old_score != new_score {
                                    let old_score_key = KEY_ENCODER.encode_txn_kv_zset_score_key(
                                        &key,
                                        old_score,
                                        &members[idx],
                                        version,
                                    );
                                    txn.del(cfs.score_cf.clone(), old_score_key)?;
                                }
                            }
                            txn.put(cfs.score_cf.clone(), score_key, member)?;
                        }
                    }

                    // update or add sub meta key
                    if added_count > 0 {
                        let sub_meta_key =
                            KEY_ENCODER.encode_txn_kv_sub_meta_key(&key, version, rand_idx);
                        let new_sub_meta_value = txn
                            .get(cfs.sub_meta_cf.clone(), sub_meta_key.clone())?
                            .map_or_else(
                                || added_count,
                                |v| {
                                    let old_sub_meta_value =
                                        i64::from_be_bytes(v.try_into().unwrap());
                                    old_sub_meta_value + added_count
                                },
                            );
                        txn.put(
                            cfs.sub_meta_cf.clone(),
                            sub_meta_key,
                            new_sub_meta_value.to_be_bytes().to_vec(),
                        )?;
                    }

                    // add meta key if key expired above
                    if expired {
                        let new_meta_value =
                            KEY_ENCODER.encode_txn_kv_zset_meta_value(ttl, version, 0);
                        txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;
                    }

                    if changed_only {
                        Ok(updated_count)
                    } else {
                        Ok(added_count)
                    }
                }
                None => {
                    let version = get_version_for_new(
                        txn,
                        cfs.gc_cf.clone(),
                        cfs.gc_version_cf.clone(),
                        &key,
                    )?;

                    if let Some(ex) = exists {
                        if ex {
                            // xx flag specified, do not create new key
                            return Ok(0);
                        }
                    }
                    // create new key
                    for idx in 0..members.len() {
                        let data_key =
                            KEY_ENCODER.encode_txn_kv_zset_data_key(&key, &members[idx], version);
                        let score = scores[idx];
                        let member = members[idx].clone();
                        let score_key =
                            KEY_ENCODER.encode_txn_kv_zset_score_key(&key, score, &member, version);
                        // add data key and score key
                        let data_value = KEY_ENCODER.encode_txn_kv_zset_data_value(score);
                        txn.put(cfs.data_cf.clone(), data_key, data_value)?;
                        // TODO check old score key exists, in case of zadd same field with different scores?
                        txn.put(cfs.score_cf.clone(), score_key, member)?;
                    }
                    // add sub meta key
                    let sub_meta_key =
                        KEY_ENCODER.encode_txn_kv_sub_meta_key(&key, version, rand_idx);
                    txn.put(
                        cfs.sub_meta_cf.clone(),
                        sub_meta_key,
                        (members.len() as i64).to_be_bytes().to_vec(),
                    )?;
                    // add meta key
                    let size = members.len() as i64;
                    let new_meta_value = KEY_ENCODER.encode_txn_kv_zset_meta_value(0, version, 0);
                    txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;
                    Ok(size)
                }
            }
        });

        match resp {
            Ok(v) => Ok(resp_int(v)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn zcard(self, key: &str) -> RocksResult<Frame> {
        let client = get_client();
        let cfs = ZsetCF::new(&client);
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.clone().txn_expire_if_needed(txn, &client, &key)?;
                        return Ok(resp_int(0));
                    }

                    let size = self.sum_key_size(&key, version)?;
                    Ok(resp_int(size))
                }
                None => Ok(resp_int(0)),
            }
        })
    }

    pub async fn zscore(self, key: &str, member: &str) -> RocksResult<Frame> {
        let client = get_client();
        let cfs = ZsetCF::new(&client);
        let key = key.to_owned();
        let member = member.to_owned();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.clone().txn_expire_if_needed(txn, &client, &key)?;
                        return Ok(resp_nil());
                    }

                    let data_key = KEY_ENCODER.encode_txn_kv_zset_data_key(&key, &member, version);
                    match txn.get(cfs.data_cf.clone(), data_key)? {
                        Some(data_value) => {
                            let score = KeyDecoder::decode_key_zset_data_value(&data_value);
                            Ok(resp_bulk(score.to_string().as_bytes().to_vec()))
                        }
                        None => Ok(resp_nil()),
                    }
                }
                None => Ok(resp_nil()),
            }
        })
    }

    pub async fn zcount(
        self,
        key: &str,
        min: f64,
        min_inclusive: bool,
        max: f64,
        max_inclusive: bool,
    ) -> RocksResult<Frame> {
        let client = get_client();
        let cfs = ZsetCF::new(&client);
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.clone().txn_expire_if_needed(txn, &client, &key)?;
                        return Ok(resp_int(0));
                    }

                    if min > max {
                        return Ok(resp_int(0));
                    }

                    let start_key = KEY_ENCODER.encode_txn_kv_zset_score_key_score_start(
                        &key,
                        min,
                        min_inclusive,
                        version,
                    );
                    let end_key = KEY_ENCODER.encode_txn_kv_zset_score_key_score_end(
                        &key,
                        max,
                        max_inclusive,
                        version,
                    );
                    let range = start_key..=end_key;
                    let bound_range: BoundRange = range.into();
                    let iter = txn.scan(cfs.score_cf.clone(), bound_range, u32::MAX)?;

                    Ok(resp_int(iter.count() as i64))
                }
                None => Ok(resp_int(0)),
            }
        })
    }

    pub async fn zrange(
        self,
        key: &str,
        mut min: i64,
        mut max: i64,
        with_scores: bool,
        reverse: bool,
    ) -> RocksResult<Frame> {
        let client = get_client();
        let cfs = ZsetCF::new(&client);
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);

        client.exec_txn(|txn| {
            let mut resp = vec![];
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.clone().txn_expire_if_needed(txn, &client, &key)?;
                        return Ok(resp_array(resp));
                    }

                    let size = self.sum_key_size(&key, version)?;
                    // convert index to positive if negtive
                    if min < 0 {
                        min += size;
                    }
                    if max < 0 {
                        max += size;
                    }

                    if reverse {
                        let r_min = size as i64 - max - 1;
                        let r_max = size as i64 - min - 1;
                        min = r_min;
                        max = r_max;
                    }

                    let bound_range = KEY_ENCODER.encode_txn_kv_zset_score_key_range(&key, version);
                    let iter =
                        txn.scan(cfs.score_cf.clone(), bound_range, size.try_into().unwrap())?;

                    let mut idx = 0;
                    for kv in iter {
                        if idx < min {
                            idx += 1;
                            continue;
                        }
                        if idx > max {
                            break;
                        }
                        idx += 1;

                        // decode member key from data key
                        let member = kv.1;
                        if reverse {
                            resp.insert(0, resp_bulk(member));
                        } else {
                            resp.push(resp_bulk(member));
                        }
                        if with_scores {
                            // decode vec[u8] to f64
                            let score = KeyDecoder::decode_key_zset_score_from_scorekey(&key, kv.0);
                            if reverse {
                                resp.insert(1, resp_bulk(score.to_string().as_bytes().to_vec()));
                            } else {
                                resp.push(resp_bulk(score.to_string().as_bytes().to_vec()));
                            }
                        }
                    }
                    Ok(resp_array(resp))
                }
                None => Ok(resp_array(resp)),
            }
        })
    }

    fn sum_key_size(&self, key: &str, version: u16) -> RocksResult<i64> {
        let client = get_client();
        let cfs = ZsetCF::new(&client);
        let key = key.to_owned();

        client.exec_txn(move |txn| {
            // check if meta key exists or already expired
            let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);
            match txn.get(cfs.meta_cf, meta_key)? {
                Some(meta_value) => {
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let bound_range = KEY_ENCODER.encode_txn_kv_sub_meta_key_range(&key, version);
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

impl RocksCommand for ZsetCommand {
    fn txn_del(
        &self,
        txn: &RocksTransaction,
        client: &RocksRawClient,
        key: &str,
    ) -> RocksResult<()> {
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);
        let cfs = ZsetCF::new(client);

        match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
            Some(meta_value) => {
                let version = KeyDecoder::decode_key_version(&meta_value);
                let size = self.sum_key_size(&key, version)?;

                if size > async_del_zset_threshold_or_default() as i64 {
                    // async del zset
                    txn.del(cfs.meta_cf.clone(), meta_key)?;

                    let gc_key = KEY_ENCODER.encode_txn_kv_gc_key(&key);
                    txn.put(cfs.gc_cf.clone(), gc_key, version.to_be_bytes().to_vec())?;

                    let gc_version_key = KEY_ENCODER.encode_txn_kv_gc_version_key(&key, version);
                    txn.put(
                        cfs.gc_version_cf.clone(),
                        gc_version_key,
                        vec![KEY_ENCODER.get_type_bytes(DataType::Zset)],
                    )?;
                } else {
                    let bound_range = KEY_ENCODER.encode_txn_kv_zset_data_key_range(&key, version);
                    let iter = txn.scan(cfs.data_cf.clone(), bound_range, u32::MAX)?;
                    for kv in iter {
                        // kv.0 is member key
                        // kv.1 is score
                        // decode the score vec to i64
                        let score = KeyDecoder::decode_key_zset_data_value(&kv.1);

                        // decode member from data key
                        let member_vec =
                            KeyDecoder::decode_key_zset_member_from_datakey(&key, kv.0.clone());
                        let member = String::from_utf8_lossy(&member_vec);

                        // remove member and score key
                        let score_key =
                            KEY_ENCODER.encode_txn_kv_zset_score_key(&key, score, &member, version);
                        txn.del(cfs.data_cf.clone(), kv.0)?;
                        txn.del(cfs.score_cf.clone(), score_key)?;
                    }

                    // delete all sub meta keys
                    let bound_range = KEY_ENCODER.encode_txn_kv_sub_meta_key_range(&key, version);
                    let iter = txn.scan_keys(cfs.sub_meta_cf.clone(), bound_range, u32::MAX)?;
                    for k in iter {
                        txn.del(cfs.sub_meta_cf.clone(), k)?;
                    }
                    txn.del(cfs.meta_cf.clone(), meta_key)?;
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn txn_expire_if_needed(
        &self,
        txn: &RocksTransaction,
        client: &RocksRawClient,
        key: &str,
    ) -> RocksResult<i64> {
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);
        let cfs = ZsetCF::new(client);

        match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
            Some(meta_value) => {
                let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                if !key_is_expired(ttl) {
                    return Ok(0);
                }

                let version = KeyDecoder::decode_key_version(&meta_value);

                let size = self.sum_key_size(&key, version)?;

                if size > async_expire_zset_threshold_or_default() as i64 {
                    // async del zset
                    txn.del(cfs.meta_cf.clone(), meta_key)?;

                    let gc_key = KEY_ENCODER.encode_txn_kv_gc_key(&key);
                    txn.put(cfs.gc_cf.clone(), gc_key, version.to_be_bytes().to_vec())?;

                    let gc_version_key = KEY_ENCODER.encode_txn_kv_gc_version_key(&key, version);
                    txn.put(
                        cfs.gc_version_cf.clone(),
                        gc_version_key,
                        vec![KEY_ENCODER.get_type_bytes(DataType::Zset)],
                    )?;
                } else {
                    let bound_range = KEY_ENCODER.encode_txn_kv_zset_data_key_range(&key, version);
                    let iter = txn.scan(cfs.data_cf.clone(), bound_range, u32::MAX)?;
                    for kv in iter {
                        // kv.0 is member key
                        // kv.1 is score
                        // decode the score vec to i64
                        let score = KeyDecoder::decode_key_zset_data_value(&kv.1);

                        // decode member from data key
                        let member_vec =
                            KeyDecoder::decode_key_zset_member_from_datakey(&key, kv.0.clone());
                        let member = String::from_utf8_lossy(&member_vec);

                        // remove member and score key
                        let score_key =
                            KEY_ENCODER.encode_txn_kv_zset_score_key(&key, score, &member, version);
                        txn.del(cfs.data_cf.clone(), kv.0)?;
                        txn.del(cfs.score_cf.clone(), score_key)?;
                    }

                    // delete all sub meta keys
                    let bound_range = KEY_ENCODER.encode_txn_kv_sub_meta_key_range(&key, version);
                    let iter = txn.scan_keys(cfs.sub_meta_cf.clone(), bound_range, u32::MAX)?;
                    for k in iter {
                        txn.del(cfs.sub_meta_cf.clone(), k)?;
                    }
                    txn.del(cfs.meta_cf.clone(), meta_key)?;
                }
                REMOVED_EXPIRED_KEY_COUNTER
                    .with_label_values(&["set"])
                    .inc();
                Ok(1)
            }
            None => Ok(0),
        }
    }

    fn txn_expire(
        &self,
        txn: &RocksTransaction,
        client: &RocksRawClient,
        key: &str,
        timestamp: i64,
        meta_value: &Value,
    ) -> RocksResult<i64> {
        let cfs = ZsetCF::new(client);
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(key);
        let ttl = KeyDecoder::decode_key_ttl(meta_value);
        if key_is_expired(ttl) {
            self.txn_expire_if_needed(txn, client, key)?;
            return Ok(0);
        }
        let version = KeyDecoder::decode_key_version(meta_value);
        let new_meta_value = KEY_ENCODER.encode_txn_kv_zset_meta_value(timestamp, version, 0);
        txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;
        Ok(1)
    }

    fn txn_gc(
        &self,
        txn: &RocksTransaction,
        client: &RocksRawClient,
        key: &str,
        version: u16,
    ) -> RocksResult<()> {
        let cfs = ZsetCF::new(client);
        // delete all sub meta key of this key and version
        let bound_range = KEY_ENCODER.encode_txn_kv_sub_meta_key_range(key, version);
        let iter = txn.scan_keys(cfs.sub_meta_cf.clone(), bound_range, u32::MAX)?;
        for k in iter {
            txn.del(cfs.sub_meta_cf.clone(), k)?;
        }

        // delete all score key of this key and version
        let bound_range = KEY_ENCODER.encode_txn_kv_zset_score_key_range(key, version);
        let iter = txn.scan_keys(cfs.score_cf.clone(), bound_range, u32::MAX)?;
        for k in iter {
            txn.del(cfs.score_cf.clone(), k)?;
        }

        // delete all data key of this key and version
        let bound_range = KEY_ENCODER.encode_txn_kv_zset_data_key_range(key, version);
        let iter = txn.scan_keys(cfs.data_cf.clone(), bound_range, u32::MAX)?;
        for k in iter {
            txn.del(cfs.data_cf.clone(), k)?;
        }
        Ok(())
    }
}
