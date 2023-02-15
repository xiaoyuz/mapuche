use crate::config::async_del_list_threshold_or_default;
use crate::metrics::REMOVED_EXPIRED_KEY_COUNTER;
use crate::rocks::client::{get_version_for_new, RocksRawClient};
use crate::rocks::encoding::{DataType, KeyDecoder};
use crate::rocks::errors::REDIS_WRONG_TYPE_ERR;
use crate::rocks::kv::bound_range::BoundRange;
use crate::rocks::kv::key::Key;
use crate::rocks::kv::value::Value;
use crate::rocks::transaction::RocksTransaction;
use crate::rocks::{
    get_client, Result as RocksResult, RocksCommand, CF_NAME_GC, CF_NAME_GC_VERSION,
    CF_NAME_LIST_DATA, CF_NAME_META, KEY_ENCODER,
};
use crate::utils::{key_is_expired, resp_array, resp_bulk, resp_err, resp_int, resp_nil, resp_ok};
use crate::Frame;
use bytes::Bytes;
use rocksdb::ColumnFamilyRef;
use std::ops::RangeFrom;

const INIT_INDEX: u64 = 1 << 32;

pub struct ListCF<'a> {
    meta_cf: ColumnFamilyRef<'a>,
    gc_cf: ColumnFamilyRef<'a>,
    gc_version_cf: ColumnFamilyRef<'a>,
    data_cf: ColumnFamilyRef<'a>,
}

impl<'a> ListCF<'a> {
    pub fn new(client: &'a RocksRawClient) -> Self {
        ListCF {
            meta_cf: client.cf_handle(CF_NAME_META).unwrap(),
            gc_cf: client.cf_handle(CF_NAME_GC).unwrap(),
            gc_version_cf: client.cf_handle(CF_NAME_GC_VERSION).unwrap(),
            data_cf: client.cf_handle(CF_NAME_LIST_DATA).unwrap(),
        }
    }
}

#[derive(Clone)]
pub struct ListCommand;

impl ListCommand {
    pub async fn push(self, key: &str, values: &Vec<Bytes>, op_left: bool) -> RocksResult<Frame> {
        let client = get_client();
        let cfs = ListCF::new(&client);
        let key = key.to_owned();
        let values = values.to_owned();

        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);

        let resp = client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let (ttl, mut version, mut left, mut right) =
                        KeyDecoder::decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.clone().txn_expire_if_needed(txn, &client, &key)?;
                        left = INIT_INDEX;
                        right = INIT_INDEX;
                        version = get_version_for_new(
                            txn,
                            cfs.gc_cf.clone(),
                            cfs.gc_version_cf.clone(),
                            &key,
                        )?;
                    }

                    let mut idx: u64;
                    for value in values {
                        if op_left {
                            left -= 1;
                            idx = left;
                        } else {
                            idx = right;
                            right += 1;
                        }

                        let data_key = KEY_ENCODER.encode_txn_kv_list_data_key(&key, idx, version);
                        txn.put(cfs.data_cf.clone(), data_key, value.to_vec())?;
                    }

                    // update meta key
                    let new_meta_value =
                        KEY_ENCODER.encode_txn_kv_list_meta_value(ttl, version, left, right);
                    txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;

                    Ok(right - left)
                }
                None => {
                    // get next version available for new key
                    let version = get_version_for_new(
                        txn,
                        cfs.gc_cf.clone(),
                        cfs.gc_version_cf.clone(),
                        &key,
                    )?;

                    let mut left = INIT_INDEX;
                    let mut right = INIT_INDEX;
                    let mut idx: u64;

                    for value in values {
                        if op_left {
                            left -= 1;
                            idx = left
                        } else {
                            idx = right;
                            right += 1;
                        }

                        // add data key
                        let data_key = KEY_ENCODER.encode_txn_kv_list_data_key(&key, idx, version);
                        txn.put(cfs.data_cf.clone(), data_key, value.to_vec())?;
                    }

                    // add meta key
                    let meta_value =
                        KEY_ENCODER.encode_txn_kv_list_meta_value(0, version, left, right);
                    txn.put(cfs.meta_cf.clone(), meta_key, meta_value)?;

                    Ok(right - left)
                }
            }
        });

        match resp {
            Ok(n) => Ok(resp_int(n as i64)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn pop(self, key: &str, op_left: bool, count: i64) -> RocksResult<Frame> {
        let client = get_client();
        let cfs = ListCF::new(&client);
        let key = key.to_owned();

        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);
        let resp = client.exec_txn(|txn| {
            let mut values = Vec::new();
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let (ttl, version, mut left, mut right) =
                        KeyDecoder::decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.clone().txn_expire_if_needed(txn, &client, &key)?;
                        return Ok(values);
                    }

                    let mut idx: u64;
                    if count == 1 {
                        if op_left {
                            idx = left;
                            left += 1;
                        } else {
                            right -= 1;
                            idx = right;
                        }
                        let data_key = KEY_ENCODER.encode_txn_kv_list_data_key(&key, idx, version);
                        // get data and delete
                        let value = txn
                            .get(cfs.data_cf.clone(), data_key.clone())
                            .unwrap()
                            .unwrap();
                        values.push(resp_bulk(value));

                        txn.del(cfs.data_cf.clone(), data_key)?;

                        if left == right {
                            // delete meta key
                            txn.del(cfs.meta_cf.clone(), meta_key)?;
                        } else {
                            // update meta key
                            let new_meta_value = KEY_ENCODER
                                .encode_txn_kv_list_meta_value(ttl, version, left, right);
                            txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;
                        }
                        Ok(values)
                    } else {
                        let mut real_count = count as u64;
                        if real_count > right - left {
                            real_count = right - left;
                        }

                        let mut data_keys = Vec::with_capacity(real_count as usize);
                        for _ in 0..real_count {
                            if op_left {
                                idx = left;
                                left += 1;
                            } else {
                                idx = right - 1;
                                right -= 1;
                            }
                            data_keys
                                .push(KEY_ENCODER.encode_txn_kv_list_data_key(&key, idx, version));
                        }
                        for pair in txn.batch_get(cfs.data_cf.clone(), data_keys)? {
                            values.push(resp_bulk(pair.1));
                            txn.del(cfs.data_cf.clone(), pair.0)?;
                        }

                        if left == right {
                            // all elements popped, just delete meta key
                            txn.del(cfs.meta_cf.clone(), meta_key)?;
                        } else {
                            // update meta key
                            let new_meta_value = KEY_ENCODER
                                .encode_txn_kv_list_meta_value(ttl, version, left, right);
                            txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;
                        }
                        Ok(values)
                    }
                }
                None => Ok(values),
            }
        });

        match resp {
            Ok(values) => {
                if values.is_empty() {
                    Ok(resp_nil())
                } else if values.len() == 1 {
                    Ok(values[0].clone())
                } else {
                    Ok(resp_array(values))
                }
            }
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn ltrim(self, key: &str, mut start: i64, mut end: i64) -> RocksResult<Frame> {
        let client = get_client();
        let cfs = ListCF::new(&client);
        let key = key.to_owned();

        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);
        let resp = client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let (ttl, version, mut left, mut right) =
                        KeyDecoder::decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.clone().txn_expire_if_needed(txn, &client, &key)?;
                        return Ok(());
                    }

                    // convert start and end to positive
                    let len = (right - left) as i64;
                    if start < 0 {
                        start += len;
                    }
                    if end < 0 {
                        end += len;
                    }

                    // ensure the op index valid
                    if start < 0 {
                        start = 0;
                    }
                    if start > len - 1 {
                        start = len - 1;
                    }

                    if end < 0 {
                        end = 0;
                    }
                    if end > len - 1 {
                        end = len - 1;
                    }

                    // convert to relative position
                    start += left as i64;
                    end += left as i64;

                    for idx in left..start as u64 {
                        let data_key = KEY_ENCODER.encode_txn_kv_list_data_key(&key, idx, version);
                        txn.del(cfs.data_cf.clone(), data_key)?;
                    }
                    let left_trim = start - left as i64;
                    if left_trim > 0 {
                        left += left_trim as u64;
                    }

                    // trim end+1->right
                    for idx in (end + 1) as u64..right {
                        let data_key = KEY_ENCODER.encode_txn_kv_list_data_key(&key, idx, version);
                        txn.del(cfs.data_cf.clone(), data_key)?;
                    }

                    let right_trim = right as i64 - end - 1;
                    if right_trim > 0 {
                        right -= right_trim as u64;
                    }

                    // check key if empty
                    if left >= right {
                        // delete meta key
                        txn.del(cfs.meta_cf.clone(), meta_key)?;
                    } else {
                        // update meta key
                        let new_meta_value =
                            KEY_ENCODER.encode_txn_kv_list_meta_value(ttl, version, left, right);
                        txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;
                    }
                    Ok(())
                }
                None => Ok(()),
            }
        });

        match resp {
            Ok(_) => Ok(resp_ok()),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn lrange(self, key: &str, mut r_left: i64, mut r_right: i64) -> RocksResult<Frame> {
        let client = get_client();
        let cfs = ListCF::new(&client);
        let key = key.to_owned();

        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);
        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let (ttl, version, left, right) = KeyDecoder::decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.clone().txn_expire_if_needed(txn, &client, &key)?;
                        return Ok(resp_array(vec![]));
                    }

                    let llen: i64 = (right - left) as i64;

                    // convert negative index to positive index
                    if r_left < 0 {
                        r_left += llen;
                    }
                    if r_right < 0 {
                        r_right += llen;
                    }
                    if r_left > r_right || r_left > llen {
                        return Ok(resp_array(vec![]));
                    }

                    let real_left = r_left + left as i64;
                    let mut real_length = r_right - r_left + 1;
                    if real_length > llen {
                        real_length = llen;
                    }

                    let data_key_start =
                        KEY_ENCODER.encode_txn_kv_list_data_key(&key, real_left as u64, version);
                    let range: RangeFrom<Key> = data_key_start..;
                    let from_range: BoundRange = range.into();
                    let iter = txn.scan(
                        cfs.data_cf.clone(),
                        from_range,
                        real_length.try_into().unwrap(),
                    )?;

                    let resp = iter.map(|kv| resp_bulk(kv.1)).collect();
                    Ok(resp_array(resp))
                }
                None => Ok(resp_array(vec![])),
            }
        })
    }
}

impl RocksCommand for ListCommand {
    fn txn_del(
        &self,
        txn: &RocksTransaction,
        client: &RocksRawClient,
        key: &str,
    ) -> RocksResult<()> {
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(&key);
        let cfs = ListCF::new(client);

        match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
            Some(meta_value) => {
                let (_, version, left, right) = KeyDecoder::decode_key_list_meta(&meta_value);
                let len = right - left;

                if len >= async_del_list_threshold_or_default() as u64 {
                    // async delete
                    // delete meta key and create gc key and gc version key with the version
                    txn.del(cfs.meta_cf.clone(), meta_key)?;

                    let gc_key = KEY_ENCODER.encode_txn_kv_gc_key(&key);
                    txn.put(cfs.gc_cf.clone(), gc_key, version.to_be_bytes().to_vec())?;

                    let gc_version_key = KEY_ENCODER.encode_txn_kv_gc_version_key(&key, version);
                    txn.put(
                        cfs.gc_version_cf.clone(),
                        gc_version_key,
                        vec![KEY_ENCODER.get_type_bytes(DataType::List)],
                    )?;
                } else {
                    let bound_range = KEY_ENCODER.encode_txn_kv_list_data_key_range(&key, version);
                    let iter = txn.scan_keys(cfs.data_cf.clone(), bound_range, u32::MAX)?;

                    for k in iter {
                        txn.del(cfs.data_cf.clone(), k)?;
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
        let cfs = ListCF::new(client);

        match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
            Some(meta_value) => {
                let (ttl, version, left, right) = KeyDecoder::decode_key_list_meta(&meta_value);
                if !key_is_expired(ttl) {
                    return Ok(0);
                }

                let len = right - left;
                if len >= async_del_list_threshold_or_default() as u64 {
                    // async delete
                    // delete meta key and create gc key and gc version key with the version
                    txn.del(cfs.meta_cf.clone(), meta_key)?;

                    let gc_key = KEY_ENCODER.encode_txn_kv_gc_key(&key);
                    txn.put(cfs.gc_cf.clone(), gc_key, version.to_be_bytes().to_vec())?;

                    let gc_version_key = KEY_ENCODER.encode_txn_kv_gc_version_key(&key, version);
                    txn.put(
                        cfs.gc_version_cf.clone(),
                        gc_version_key,
                        vec![KEY_ENCODER.get_type_bytes(DataType::List)],
                    )?;
                } else {
                    let bound_range = KEY_ENCODER.encode_txn_kv_list_data_key_range(&key, version);
                    let iter = txn.scan_keys(cfs.data_cf.clone(), bound_range, u32::MAX)?;

                    for k in iter {
                        txn.del(cfs.data_cf.clone(), k)?;
                    }
                    txn.del(cfs.meta_cf.clone(), meta_key)?;
                }

                REMOVED_EXPIRED_KEY_COUNTER
                    .with_label_values(&["list"])
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
        let cfs = ListCF::new(client);
        let meta_key = KEY_ENCODER.encode_txn_kv_meta_key(key);
        let ttl = KeyDecoder::decode_key_ttl(meta_value);
        if key_is_expired(ttl) {
            self.txn_expire_if_needed(txn, client, key)?;
            return Ok(0);
        }
        let (_, version, left, right) = KeyDecoder::decode_key_list_meta(meta_value);
        let new_meta_value =
            KEY_ENCODER.encode_txn_kv_list_meta_value(timestamp, version, left, right);
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
        let cfs = ListCF::new(client);
        // delete all data key of this key and version
        let bound_range = KEY_ENCODER.encode_txn_kv_list_data_key_range(key, version);
        let iter = txn.scan_keys(cfs.data_cf.clone(), bound_range, u32::MAX)?;
        for k in iter {
            txn.del(cfs.data_cf.clone(), k)?;
        }
        Ok(())
    }
}
