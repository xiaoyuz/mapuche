
use rocksdb::{ColumnFamilyRef};
use crate::config::async_expire_set_threshold_or_default;
use crate::Frame;
use crate::rocks::{CF_NAME_SET_DATA, CF_NAME_GC, CF_NAME_SET_META, CF_NAME_SET_SUB_META, gen_next_meta_index, get_client, KEY_ENCODER, Result as RocksResult};
use crate::rocks::client::{get_version_for_new, RocksRawClient};
use crate::rocks::encoding::{DataType, KeyDecoder};
use crate::rocks::errors::REDIS_WRONG_TYPE_ERR;
use crate::rocks::transaction::RocksTransaction;
use crate::utils::{count_unique_keys, key_is_expired, resp_err, resp_int};

pub struct SetCF<'a> {
    meta_cf: ColumnFamilyRef<'a>,
    sub_meta_cf: ColumnFamilyRef<'a>,
    gc_cf: ColumnFamilyRef<'a>,
    data_cf: ColumnFamilyRef<'a>,
}

impl<'a> SetCF<'a> {
    pub fn new(client: &'a RocksRawClient) -> Self {
        SetCF {
            meta_cf: client.cf_handle(CF_NAME_SET_META).unwrap(),
            sub_meta_cf: client.cf_handle(CF_NAME_SET_SUB_META).unwrap(),
            gc_cf: client.cf_handle(CF_NAME_GC).unwrap(),
            data_cf: client.cf_handle(CF_NAME_SET_DATA).unwrap(),
        }
    }
}

#[derive(Clone)]
pub struct SetCommand;

impl SetCommand {
    fn sum_key_size(
        self,
        key: &str,
        version: u16
    ) -> RocksResult<i64> {
        let client = get_client();
        let cfs = SetCF::new(&client);
        let key = key.to_owned();

        client.exec_txn(move |txn| {
            // check if meta key exists or already expired
            let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);
            match txn.get(cfs.meta_cf, meta_key)? {
                Some(meta_value) => {
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let bound_range =
                        KEY_ENCODER.encode_txn_kv_sub_meta_key_range(&key, version);
                    let iter = txn.scan(
                        cfs.sub_meta_cf.clone(),
                        bound_range,
                        u32::MAX,
                    )?;
                    let sum = iter
                        .map(|kv| i64::from_be_bytes(kv.1.try_into().unwrap()))
                        .sum();
                    Ok(sum)
                }
                None => Ok(0)
            }
        })
    }

    pub async fn sadd(self, key: &str, members: &Vec<String>) -> RocksResult<Frame> {
        let client = get_client();
        let cfs = SetCF::new(&client);
        let key = key.to_owned();
        let members = members.to_owned();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);
        let rand_idx = gen_next_meta_index();

        let resp = client.exec_txn(move |txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let mut expired = false;
                    let (ttl, mut version, _meta_size) =
                        KeyDecoder::decode_key_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.clone()
                            .txn_expire_if_needed(txn, &cfs, &key)?;
                        expired = true;
                        version = get_version_for_new(txn, cfs.gc_cf.clone(), &key)?;
                    }
                    let mut member_data_keys = Vec::with_capacity(members.len());
                    for m in &members {
                        let data_key =
                            KEY_ENCODER.encode_txn_kv_set_data_key(&key, m, version);
                        member_data_keys.push(data_key);
                    }
                    // batch get
                    // count the unique members
                    let real_member_count = count_unique_keys(&member_data_keys);
                    let values_count = txn.batch_get(cfs.data_cf.clone(), member_data_keys)?.len();
                    let added = real_member_count as i64 - values_count as i64;
                    for m in &members {
                        let data_key =
                            KEY_ENCODER.encode_txn_kv_set_data_key(&key, m, version);
                        txn.put(cfs.data_cf.clone(), data_key, vec![0])?;
                    }

                    // choose a random sub meta key for update, create if not exists
                    let sub_meta_key =
                        KEY_ENCODER.encode_txn_kv_sub_meta_key(&key, version, rand_idx);
                    let new_sub_meta_value =
                        txn.get(cfs.sub_meta_cf.clone(), sub_meta_key.clone())?.map_or_else(
                            || added,
                            |value| {
                                let old_sub_meta_value =
                                    i64::from_be_bytes(value.try_into().unwrap());
                                old_sub_meta_value + added
                            },
                        );
                    txn.put(cfs.sub_meta_cf.clone(), sub_meta_key, new_sub_meta_value.to_be_bytes().to_vec())?;

                    // create a new meta key if key already expired above
                    if expired {
                        let new_meta_value =
                            KEY_ENCODER.encode_txn_kv_set_meta_value(0, version, 0);
                        txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;
                    }

                    Ok(added)
                }
                None => {
                    let version = get_version_for_new(txn, cfs.gc_cf.clone(), &key)?;
                    // create new meta key and meta value
                    for m in &members {
                        // check member already exists
                        let data_key =
                            KEY_ENCODER.encode_txn_kv_set_data_key(&key, m, version);
                        // value can not be vec![] if use cse as backend
                        txn.put(cfs.data_cf.clone(), data_key, vec![0])?;
                    }
                    // create meta key
                    let meta_value = KEY_ENCODER.encode_txn_kv_set_meta_value(0, version, 0);
                    txn.put(cfs.meta_cf.clone(), meta_key, meta_value)?;

                    let added = count_unique_keys(&members) as i64;

                    // create sub meta key with a random index
                    let sub_meta_key =
                        KEY_ENCODER.encode_txn_kv_sub_meta_key(&key, version, rand_idx);
                    txn.put(cfs.sub_meta_cf.clone(), sub_meta_key, added.to_be_bytes().to_vec())?;
                    Ok(added)
                }
            }
        });

        match resp {
            Ok(v) => Ok(resp_int(v)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub fn txn_expire_if_needed(
        self,
        txn: &RocksTransaction,
        cfs: &SetCF,
        key: &str
    ) -> RocksResult<i64> {
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);

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

                    let gc_key = KEY_ENCODER.encode_txn_kv_gc_key(&key);
                    txn.put(cfs.gc_cf.clone(), gc_key, version.to_be_bytes().to_vec())?;

                    let gc_version_key =
                        KEY_ENCODER.encode_txn_kv_gc_version_key(&key, version);
                    txn.put(
                        cfs.gc_cf.clone(),
                        gc_version_key,
                        vec![KEY_ENCODER.get_type_bytes(DataType::Set)],
                    )?;
                } else {
                    let sub_meta_range =
                        KEY_ENCODER.encode_txn_kv_sub_meta_key_range(&key, version);

                    let iter = txn.scan_keys(
                        cfs.sub_meta_cf.clone(),
                        sub_meta_range,
                        u32::MAX,
                    )?;
                    for k in iter {
                        txn.del(cfs.sub_meta_cf.clone(), k)?;
                    }

                    let data_bound_range =
                        KEY_ENCODER.encode_txn_kv_set_data_key_range(&key, version);
                    let iter = txn.scan_keys(
                        cfs.data_cf.clone(),
                        data_bound_range,
                        u32::MAX,
                    )?;
                    for k in iter {
                        txn.del(cfs.data_cf.clone(), k)?;
                    }

                    txn.del(cfs.meta_cf.clone(), meta_key)?;
                }
                Ok(1)
            }
            None => Ok(0)
        }
    }

    pub async fn scard(self, key: &str) -> RocksResult<Frame> {
        let client = get_client();
        let cfs = SetCF::new(&client);
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(key);
        let key = key.to_owned();

        client.exec_txn(move |txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key)? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                        return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                    }

                    let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.clone()
                            .txn_expire_if_needed(txn, &cfs, &key)?;
                        return Ok(resp_int(0));
                    }

                    let size = self.sum_key_size(&key, version)?;
                    Ok(resp_int(size))
                }
                None => Ok(resp_int(0)),
            }
        })
    }
}