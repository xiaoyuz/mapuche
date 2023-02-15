use crate::config::{
    async_del_hash_threshold_or_default, async_expire_hash_threshold_or_default,
    config_meta_key_number_or_default, LOGGER,
};
use crate::metrics::REMOVED_EXPIRED_KEY_COUNTER;
use crate::rocks::client::{get_version_for_new, RocksRawClient};
use crate::rocks::encoding::{DataType, KeyDecoder};
use crate::rocks::errors::REDIS_WRONG_TYPE_ERR;
use crate::rocks::kv::kvpair::KvPair;
use crate::rocks::kv::value::Value;
use crate::rocks::transaction::RocksTransaction;
use crate::rocks::{
    gen_next_meta_index, get_client, Result as RocksResult, RocksCommand, CF_NAME_GC,
    CF_NAME_GC_VERSION, CF_NAME_HASH_DATA, CF_NAME_HASH_SUB_META, CF_NAME_META, KEY_ENCODER,
};
use crate::utils::{
    count_unique_keys, key_is_expired, resp_bulk, resp_err, resp_int, resp_nil, resp_ok,
};
use crate::Frame;
use rocksdb::ColumnFamilyRef;
use slog::debug;

pub struct HashCF<'a> {
    meta_cf: ColumnFamilyRef<'a>,
    sub_meta_cf: ColumnFamilyRef<'a>,
    gc_cf: ColumnFamilyRef<'a>,
    gc_version_cf: ColumnFamilyRef<'a>,
    data_cf: ColumnFamilyRef<'a>,
}

impl<'a> HashCF<'a> {
    pub fn new(client: &'a RocksRawClient) -> Self {
        HashCF {
            meta_cf: client.cf_handle(CF_NAME_META).unwrap(),
            sub_meta_cf: client.cf_handle(CF_NAME_HASH_SUB_META).unwrap(),
            gc_cf: client.cf_handle(CF_NAME_GC).unwrap(),
            gc_version_cf: client.cf_handle(CF_NAME_GC_VERSION).unwrap(),
            data_cf: client.cf_handle(CF_NAME_HASH_DATA).unwrap(),
        }
    }
}

#[derive(Clone)]
pub struct HashCommand;

impl HashCommand {
    pub async fn hset(
        self,
        key: &str,
        fvs: &[KvPair],
        is_hmset: bool,
        is_nx: bool,
    ) -> RocksResult<Frame> {
        let client = get_client();
        let cfs = HashCF::new(&client);
        let key = key.to_owned();
        let fvs_copy = fvs.to_vec();
        let fvs_len = fvs_copy.len();
        let idx = gen_next_meta_index();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);

        let resp = client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type is hash
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    // already exists
                    let (ttl, mut version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);

                    let mut expired = false;

                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &client, &key)?;
                        expired = true;
                        version = get_version_for_new(
                            txn,
                            cfs.gc_cf.clone(),
                            cfs.gc_version_cf.clone(),
                            &key,
                        )?;
                    } else if is_nx {
                        // when is_nx == true, fvs_len must be 1
                        let kv = fvs_copy.get(0).unwrap();
                        let field: Vec<u8> = kv.clone().0.into();
                        let datakey = KEY_ENCODER.encode_txn_kv_hash_data_key(
                            &key,
                            &String::from_utf8_lossy(&field),
                            version,
                        );
                        if txn.get(cfs.data_cf.clone(), datakey)?.is_some() {
                            return Ok(0);
                        }
                    }

                    let mut added_count = 1;
                    if !is_nx {
                        let mut fields_data_key = Vec::with_capacity(fvs_len);
                        for kv in fvs_copy.clone() {
                            let field: Vec<u8> = kv.0.into();
                            let datakey = KEY_ENCODER.encode_txn_kv_hash_data_key(
                                &key,
                                &String::from_utf8_lossy(&field),
                                version,
                            );
                            fields_data_key.push(datakey);
                        }
                        // batch get
                        let real_fields_count = count_unique_keys(&fields_data_key);
                        added_count = real_fields_count as i64
                            - txn.batch_get(cfs.data_cf.clone(), fields_data_key)?.len() as i64;
                    }

                    for kv in fvs_copy {
                        let field: Vec<u8> = kv.0.into();
                        let data_key = KEY_ENCODER.encode_txn_kv_hash_data_key(
                            &key,
                            &String::from_utf8_lossy(&field),
                            version,
                        );
                        txn.put(cfs.data_cf.clone(), data_key, kv.1)?;
                    }

                    // gerate a random index, update sub meta key, create a new sub meta key with this index
                    let sub_meta_key = KEY_ENCODER.encode_txn_kv_sub_meta_key(&key, version, idx);
                    // create or update it
                    let new_sub_meta_value = txn
                        .get(cfs.sub_meta_cf.clone(), sub_meta_key.clone())?
                        .map_or_else(
                            || added_count.to_be_bytes().to_vec(),
                            |sub_meta_value| {
                                let sub_size =
                                    i64::from_be_bytes(sub_meta_value.try_into().unwrap());

                                (sub_size + added_count).to_be_bytes().to_vec()
                            },
                        );
                    txn.put(cfs.sub_meta_cf.clone(), sub_meta_key, new_sub_meta_value)?;
                    if expired {
                        // add meta key
                        let meta_size = config_meta_key_number_or_default();
                        let new_metaval =
                            KEY_ENCODER.encode_txn_kv_hash_meta_value(ttl, version, meta_size);
                        txn.put(cfs.meta_cf.clone(), meta_key, new_metaval)?;
                    }
                }
                None => {
                    let version = get_version_for_new(
                        txn,
                        cfs.gc_cf.clone(),
                        cfs.gc_version_cf.clone(),
                        &key,
                    )?;
                    debug!(LOGGER, "hset new key {} with version: {}", key, version);

                    // not exists
                    let ttl = 0;
                    let mut fields_data_key = vec![];
                    for kv in fvs_copy.clone() {
                        let field: Vec<u8> = kv.0.into();
                        let datakey = KEY_ENCODER.encode_txn_kv_hash_data_key(
                            &key,
                            &String::from_utf8_lossy(&field),
                            version,
                        );
                        fields_data_key.push(datakey);
                    }
                    let real_fields_count = count_unique_keys(&fields_data_key);

                    for kv in fvs_copy {
                        let field: Vec<u8> = kv.0.into();
                        let datakey = KEY_ENCODER.encode_txn_kv_hash_data_key(
                            &key,
                            &String::from_utf8_lossy(&field),
                            version,
                        );
                        txn.put(cfs.data_cf.clone(), datakey, kv.1)?;
                    }

                    // set meta key
                    let meta_size = config_meta_key_number_or_default();
                    let new_metaval =
                        KEY_ENCODER.encode_txn_kv_hash_meta_value(ttl, version, meta_size);
                    txn.put(cfs.meta_cf.clone(), meta_key, new_metaval)?;

                    // set sub meta key with a random index
                    let sub_meta_key = KEY_ENCODER.encode_txn_kv_sub_meta_key(&key, version, idx);
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
        let client = get_client();
        let cfs = HashCF::new(&client);
        let key = key.to_owned();
        let field = field.to_owned();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type is hash
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);

                    if key_is_expired(ttl) {
                        self.clone().txn_expire_if_needed(txn, &client, &key)?;
                        return Ok(resp_nil());
                    }

                    let data_key = KEY_ENCODER.encode_txn_kv_hash_data_key(&key, &field, version);

                    txn.get(cfs.data_cf.clone(), data_key)?
                        .map_or_else(|| Ok(resp_nil()), |data| Ok(resp_bulk(data)))
                }
                None => Ok(resp_nil()),
            }
        })
    }

    pub async fn hstrlen(self, key: &str, field: &str) -> RocksResult<Frame> {
        let client = get_client();
        let cfs = HashCF::new(&client);
        let key = key.to_owned();
        let field = field.to_owned();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);

        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type is hash
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }

                    let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);

                    if key_is_expired(ttl) {
                        self.clone().txn_expire_if_needed(txn, &client, &key)?;
                        return Ok(resp_int(0));
                    }

                    let data_key = KEY_ENCODER.encode_txn_kv_hash_data_key(&key, &field, version);

                    txn.get(cfs.data_cf.clone(), data_key)?
                        .map_or_else(|| Ok(resp_int(0)), |data| Ok(resp_int(data.len() as i64)))
                }
                None => Ok(resp_int(0)),
            }
        })
    }

    fn sum_key_size(&self, key: &str, version: u16) -> RocksResult<i64> {
        let client = get_client();
        let cfs = HashCF::new(&client);
        let key = key.to_owned();

        client.exec_txn(move |txn| {
            // check if meta key exists or already expired
            let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);
            match txn.get(cfs.meta_cf, meta_key)? {
                Some(meta_value) => {
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
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

impl RocksCommand for HashCommand {
    fn txn_del(
        &self,
        txn: &RocksTransaction,
        client: &RocksRawClient,
        key: &str,
    ) -> RocksResult<()> {
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);
        let cfs = HashCF::new(client);

        match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
            Some(meta_value) => {
                let (_, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                let meta_size = self.sum_key_size(&key, version)?;

                if meta_size > async_del_hash_threshold_or_default() as i64 {
                    // do async del
                    txn.del(cfs.meta_cf.clone(), meta_key)?;

                    let gc_key = KEY_ENCODER.encode_txn_kv_gc_key(&key);
                    txn.put(cfs.gc_cf.clone(), gc_key, version.to_be_bytes().to_vec())?;

                    let gc_version_key = KEY_ENCODER.encode_txn_kv_gc_version_key(&key, version);
                    txn.put(
                        cfs.gc_version_cf.clone(),
                        gc_version_key,
                        vec![KEY_ENCODER.get_type_bytes(DataType::Hash)],
                    )?;
                } else {
                    let bound_range = KEY_ENCODER.encode_txn_kv_hash_data_key_range(&key, version);
                    // scan return iterator
                    let iter = txn.scan_keys(cfs.data_cf.clone(), bound_range, u32::MAX)?;

                    for k in iter {
                        txn.del(cfs.data_cf.clone(), k)?;
                    }

                    let sub_meta_bound_range =
                        KEY_ENCODER.encode_txn_kv_sub_meta_key_range(&key, version);
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

    fn txn_expire_if_needed(
        &self,
        txn: &RocksTransaction,
        client: &RocksRawClient,
        key: &str,
    ) -> RocksResult<i64> {
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);
        let cfs = HashCF::new(client);

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

                    let gc_key = KEY_ENCODER.encode_txn_kv_gc_key(&key);
                    txn.put(cfs.gc_cf.clone(), gc_key, version.to_be_bytes().to_vec())?;

                    let gc_version_key = KEY_ENCODER.encode_txn_kv_gc_version_key(&key, version);
                    txn.put(
                        cfs.gc_version_cf.clone(),
                        gc_version_key,
                        vec![KEY_ENCODER.get_type_bytes(DataType::Hash)],
                    )?;
                } else {
                    let bound_range = KEY_ENCODER.encode_txn_kv_hash_data_key_range(&key, version);
                    // scan return iterator
                    let iter = txn.scan_keys(cfs.data_cf.clone(), bound_range, u32::MAX)?;

                    for k in iter {
                        txn.del(cfs.data_cf.clone(), k)?;
                    }

                    let sub_meta_bound_range =
                        KEY_ENCODER.encode_txn_kv_sub_meta_key_range(&key, version);
                    let sub_meta_iter =
                        txn.scan_keys(cfs.sub_meta_cf.clone(), sub_meta_bound_range, u32::MAX)?;
                    for k in sub_meta_iter {
                        txn.del(cfs.sub_meta_cf.clone(), k)?;
                    }

                    txn.del(cfs.meta_cf.clone(), meta_key)?;
                }
                REMOVED_EXPIRED_KEY_COUNTER
                    .with_label_values(&["hash"])
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
        let cfs = HashCF::new(client);
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(key);
        let ttl = KeyDecoder::decode_key_ttl(meta_value);
        if key_is_expired(ttl) {
            self.txn_expire_if_needed(txn, client, key)?;
            return Ok(0);
        }
        let version = KeyDecoder::decode_key_version(meta_value);
        let new_meta_value = KEY_ENCODER.encode_txn_kv_hash_meta_value(timestamp, version, 0);
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
        let cfs = HashCF::new(client);
        // delete all sub meta key of this key and version
        let bound_range = KEY_ENCODER.encode_txn_kv_sub_meta_key_range(key, version);
        let iter = txn.scan_keys(cfs.sub_meta_cf.clone(), bound_range, u32::MAX)?;
        for k in iter {
            txn.del(cfs.sub_meta_cf.clone(), k)?;
        }
        // delete all data key of this key and version
        let bound_range = KEY_ENCODER.encode_txn_kv_hash_data_key_range(key, version);
        let iter = txn.scan_keys(cfs.data_cf.clone(), bound_range, u32::MAX)?;
        for k in iter {
            txn.del(cfs.data_cf.clone(), k)?;
        }
        Ok(())
    }
}
