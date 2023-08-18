use crate::config::{
    async_del_list_threshold_or_default, async_expire_list_threshold_or_default,
    cmd_linsert_length_limit_or_default, cmd_lrem_length_limit_or_default,
};
use crate::db::DBInner;
use crate::rocks::client::{get_version_for_new, RocksClient};
use crate::rocks::encoding::{DataType, KeyDecoder};
use crate::rocks::errors::{
    REDIS_INDEX_OUT_OF_RANGE_ERR, REDIS_LIST_TOO_LARGE_ERR, REDIS_NO_SUCH_KEY_ERR,
    REDIS_WRONG_TYPE_ERR,
};
use crate::rocks::kv::bound_range::BoundRange;
use crate::rocks::kv::key::Key;
use crate::rocks::kv::value::Value;
use crate::rocks::transaction::RocksTransaction;
use crate::rocks::{
    Result as RocksResult, TxnCommand, CF_NAME_GC, CF_NAME_GC_VERSION, CF_NAME_LIST_DATA,
    CF_NAME_META,
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
    pub fn new(client: &'a RocksClient) -> Self {
        ListCF {
            meta_cf: client.cf_handle(CF_NAME_META).unwrap(),
            gc_cf: client.cf_handle(CF_NAME_GC).unwrap(),
            gc_version_cf: client.cf_handle(CF_NAME_GC_VERSION).unwrap(),
            data_cf: client.cf_handle(CF_NAME_LIST_DATA).unwrap(),
        }
    }
}

pub struct ListCommand<'a> {
    inner_db: &'a DBInner,
}

impl<'a> ListCommand<'a> {
    pub fn new(inner_db: &'a DBInner) -> Self {
        Self { inner_db }
    }

    pub async fn push(self, key: &str, values: &Vec<Bytes>, op_left: bool) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = ListCF::new(client);
        let key = key.to_owned();
        let values = values.to_owned();

        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);

        let resp = client.exec_txn(|txn| {
            match txn.get_for_update(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let (ttl, mut version, mut left, mut right) =
                        KeyDecoder::decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        left = INIT_INDEX;
                        right = INIT_INDEX;
                        version = get_version_for_new(
                            txn,
                            cfs.gc_cf.clone(),
                            cfs.gc_version_cf.clone(),
                            &key,
                            &self.inner_db.key_encoder,
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

                        let data_key = self
                            .inner_db
                            .key_encoder
                            .encode_list_data_key(&key, idx, version);
                        txn.put(cfs.data_cf.clone(), data_key, value.to_vec())?;
                    }

                    // update meta key
                    let new_meta_value = self
                        .inner_db
                        .key_encoder
                        .encode_list_meta_value(ttl, version, left, right);
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
                        &self.inner_db.key_encoder,
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
                        let data_key = self
                            .inner_db
                            .key_encoder
                            .encode_list_data_key(&key, idx, version);
                        txn.put(cfs.data_cf.clone(), data_key, value.to_vec())?;
                    }

                    // add meta key
                    let meta_value = self
                        .inner_db
                        .key_encoder
                        .encode_list_meta_value(0, version, left, right);
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
        let client = &self.inner_db.client;
        let cfs = ListCF::new(client);
        let key = key.to_owned();

        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        let resp = client.exec_txn(|txn| {
            let mut values = Vec::new();
            match txn.get_for_update(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let (ttl, version, mut left, mut right) =
                        KeyDecoder::decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
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
                        let data_key = self
                            .inner_db
                            .key_encoder
                            .encode_list_data_key(&key, idx, version);
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
                            let new_meta_value = self
                                .inner_db
                                .key_encoder
                                .encode_list_meta_value(ttl, version, left, right);
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
                            data_keys.push(
                                self.inner_db
                                    .key_encoder
                                    .encode_list_data_key(&key, idx, version),
                            );
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
                            let new_meta_value = self
                                .inner_db
                                .key_encoder
                                .encode_list_meta_value(ttl, version, left, right);
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
        let client = &self.inner_db.client;
        let cfs = ListCF::new(client);
        let key = key.to_owned();

        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        let resp = client.exec_txn(|txn| {
            match txn.get_for_update(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let (ttl, version, mut left, mut right) =
                        KeyDecoder::decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
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
                        let data_key = self
                            .inner_db
                            .key_encoder
                            .encode_list_data_key(&key, idx, version);
                        txn.del(cfs.data_cf.clone(), data_key)?;
                    }
                    let left_trim = start - left as i64;
                    if left_trim > 0 {
                        left += left_trim as u64;
                    }

                    // trim end+1->right
                    for idx in (end + 1) as u64..right {
                        let data_key = self
                            .inner_db
                            .key_encoder
                            .encode_list_data_key(&key, idx, version);
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
                        let new_meta_value = self
                            .inner_db
                            .key_encoder
                            .encode_list_meta_value(ttl, version, left, right);
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
        let client = &self.inner_db.client;
        let cfs = ListCF::new(client);
        let key = key.to_owned();

        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let (ttl, version, left, right) = KeyDecoder::decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
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

                    let data_key_start = self.inner_db.key_encoder.encode_list_data_key(
                        &key,
                        real_left as u64,
                        version,
                    );
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

    pub async fn llen(self, key: &str) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = ListCF::new(client);
        let key = key.to_owned();

        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let (ttl, _version, left, right) =
                        KeyDecoder::decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(resp_int(0));
                    }

                    let llen: i64 = (right - left) as i64;
                    Ok(resp_int(llen))
                }
                None => Ok(resp_int(0)),
            }
        })
    }

    pub async fn lindex(self, key: &str, mut idx: i64) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = ListCF::new(client);
        let key = key.to_owned();

        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        client.exec_txn(|txn| {
            match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let (ttl, version, left, right) = KeyDecoder::decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(resp_nil());
                    }

                    let len = right - left;
                    // try convert idx to positive if needed
                    if idx < 0 {
                        idx += len as i64;
                    }

                    let real_idx = left as i64 + idx;

                    // get value from data key
                    let data_key = self.inner_db.key_encoder.encode_list_data_key(
                        &key,
                        real_idx as u64,
                        version,
                    );
                    if let Some(value) = txn.get(cfs.data_cf.clone(), data_key)? {
                        Ok(resp_bulk(value))
                    } else {
                        Ok(resp_nil())
                    }
                }
                None => Ok(resp_nil()),
            }
        })
    }

    pub async fn lset(self, key: &str, mut idx: i64, ele: &Bytes) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = ListCF::new(client);
        let key = key.to_owned();
        let ele = ele.to_owned();

        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        let resp = client.exec_txn(|txn| {
            match txn.get_for_update(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let (ttl, version, left, right) = KeyDecoder::decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Err(REDIS_NO_SUCH_KEY_ERR);
                    }

                    // convert idx to positive is needed
                    if idx < 0 {
                        idx += (right - left) as i64;
                    }

                    let uidx = idx + left as i64;
                    if idx < 0 || uidx < left as i64 || uidx > (right - 1) as i64 {
                        return Err(REDIS_INDEX_OUT_OF_RANGE_ERR);
                    }

                    let data_key =
                        self.inner_db
                            .key_encoder
                            .encode_list_data_key(&key, uidx as u64, version);
                    // data keys exists, update it to new value
                    txn.put(cfs.data_cf.clone(), data_key, ele.to_vec())?;
                    Ok(())
                }
                None => Err(REDIS_NO_SUCH_KEY_ERR),
            }
        });

        match resp {
            Ok(_) => Ok(resp_ok()),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn linsert(
        self,
        key: &str,
        before_pivot: bool,
        pivot: &Bytes,
        element: &Bytes,
    ) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = ListCF::new(client);
        let key = key.to_owned();
        let pivot = pivot.to_owned();
        let element = element.to_owned();

        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        let resp = client.exec_txn(|txn| {
            match txn.get_for_update(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let (ttl, version, mut left, mut right) =
                        KeyDecoder::decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(0);
                    }

                    // check list length is not too long
                    let limit_len = cmd_linsert_length_limit_or_default();
                    if limit_len > 0 && right - left > limit_len as u64 {
                        return Err(REDIS_LIST_TOO_LARGE_ERR);
                    }

                    // get list items bound range
                    let bound_range = self
                        .inner_db
                        .key_encoder
                        .encode_list_data_key_range(&key, version);

                    // iter will only return the matched kvpair
                    let mut iter = txn
                        .scan(cfs.data_cf.clone(), bound_range, u32::MAX)?
                        .filter(|kv| {
                            if kv.1 == pivot.to_vec() {
                                return true;
                            }
                            false
                        });

                    // yeild the first matched kvpair
                    if let Some(kv) = iter.next() {
                        // decode the idx from data key
                        let idx = KeyDecoder::decode_key_list_idx_from_datakey(&key, kv.0);

                        // compare the pivot distance to left and right, choose the shorter one
                        let from_left = idx - left < right - idx;

                        let idx_op;
                        if from_left {
                            idx_op = if before_pivot { idx - 1 } else { idx };
                            // move data key from left to left-1
                            // move backwards for elements in idx [left, idx_op], add the new element to idx_op
                            if idx_op >= left {
                                let left_range = self
                                    .inner_db
                                    .key_encoder
                                    .encode_list_data_key_idx_range(&key, left, idx_op, version);
                                let iter = txn.scan(cfs.data_cf.clone(), left_range, u32::MAX)?;

                                for kv in iter {
                                    let key_idx =
                                        KeyDecoder::decode_key_list_idx_from_datakey(&key, kv.0);
                                    let new_data_key = self
                                        .inner_db
                                        .key_encoder
                                        .encode_list_data_key(&key, key_idx - 1, version);
                                    txn.put(cfs.data_cf.clone(), new_data_key, kv.1)?;
                                }
                            }

                            left -= 1;
                        } else {
                            idx_op = if before_pivot { idx } else { idx + 1 };
                            // move data key from right to right+1
                            // move forwards for elements in idx [idx_op, right-1], add the new element to idx_op
                            // if idx_op == right, no need to move data key
                            if idx_op < right {
                                let right_range =
                                    self.inner_db.key_encoder.encode_list_data_key_idx_range(
                                        &key,
                                        idx_op,
                                        right - 1,
                                        version,
                                    );
                                let iter = txn.scan(cfs.data_cf.clone(), right_range, u32::MAX)?;

                                for kv in iter {
                                    let key_idx =
                                        KeyDecoder::decode_key_list_idx_from_datakey(&key, kv.0);
                                    let new_data_key = self
                                        .inner_db
                                        .key_encoder
                                        .encode_list_data_key(&key, key_idx + 1, version);
                                    txn.put(cfs.data_cf.clone(), new_data_key, kv.1)?;
                                }
                            }

                            right += 1;
                        }

                        // fill the pivot
                        let pivot_data_key = self
                            .inner_db
                            .key_encoder
                            .encode_list_data_key(&key, idx_op, version);
                        txn.put(cfs.data_cf.clone(), pivot_data_key, element.to_vec())?;

                        // update meta key
                        let new_meta_value = self
                            .inner_db
                            .key_encoder
                            .encode_list_meta_value(ttl, version, left, right);
                        txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;

                        let len = (right - left) as i64;
                        Ok(len)
                    } else {
                        // no matched pivot, ignore
                        Ok(-1)
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

    pub async fn lrem(
        self,
        key: &str,
        count: usize,
        from_head: bool,
        ele: &Bytes,
    ) -> RocksResult<Frame> {
        let client = &self.inner_db.client;
        let cfs = ListCF::new(client);
        let key = key.to_owned();
        let ele = ele.to_owned();

        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        let resp = client.exec_txn(|txn| {
            match txn.get_for_update(cfs.meta_cf.clone(), meta_key.clone())? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                        return Err(REDIS_WRONG_TYPE_ERR);
                    }
                    let (ttl, version, left, right) = KeyDecoder::decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        self.txn_expire_if_needed(txn, &key)?;
                        return Ok(0);
                    }

                    let len = right - left;

                    // check list length is not too long
                    let limit_len = cmd_lrem_length_limit_or_default();
                    if limit_len > 0 && len > limit_len as u64 {
                        return Err(REDIS_LIST_TOO_LARGE_ERR);
                    }

                    // get list items bound range
                    let bound_range = self
                        .inner_db
                        .key_encoder
                        .encode_list_data_key_range(&key, version);

                    // iter will only return the matched kvpair
                    let iter = txn
                        .scan(cfs.data_cf.clone(), bound_range.clone(), u32::MAX)?
                        .filter(|kv| {
                            if kv.1 == ele.to_vec() {
                                return true;
                            }
                            false
                        });

                    // hole saves the elements to be removed in order
                    let mut hole: Vec<u64> = iter
                        .map(|kv| KeyDecoder::decode_key_list_idx_from_datakey(&key, kv.0))
                        .collect();

                    // no matched element, return 0
                    if hole.is_empty() {
                        return Ok(0);
                    }

                    if !from_head {
                        hole.reverse();
                    }

                    let mut removed_count = 0;

                    if from_head {
                        let iter = txn.scan(cfs.data_cf.clone(), bound_range, u32::MAX)?;

                        for kv in iter {
                            let key_idx =
                                KeyDecoder::decode_key_list_idx_from_datakey(&key, kv.0.clone());
                            if hole.is_empty() {
                                break;
                            }

                            if !((count > 0 && removed_count == count)
                                || removed_count == hole.len())
                                && hole[removed_count] == key_idx
                            {
                                txn.del(cfs.data_cf.clone(), kv.0)?;
                                removed_count += 1;
                                continue;
                            }

                            // check if key idx need to be backward move
                            if removed_count > 0 {
                                let new_data_key = self.inner_db.key_encoder.encode_list_data_key(
                                    &key,
                                    key_idx - removed_count as u64,
                                    version,
                                );
                                txn.put(cfs.data_cf.clone(), new_data_key, kv.1)?;
                                txn.del(cfs.data_cf.clone(), kv.0)?;
                            }
                        }

                        // update meta key or delete it if no element left
                        if len == removed_count as u64 {
                            txn.del(cfs.meta_cf.clone(), meta_key)?;
                        } else {
                            let new_meta_value = self.inner_db.key_encoder.encode_list_meta_value(
                                ttl,
                                version,
                                left,
                                right - removed_count as u64,
                            );
                            txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;
                        }
                    } else {
                        let iter = txn.scan_reverse(cfs.data_cf.clone(), bound_range, u32::MAX)?;

                        for kv in iter {
                            let key_idx =
                                KeyDecoder::decode_key_list_idx_from_datakey(&key, kv.0.clone());
                            if hole.is_empty() {
                                break;
                            }

                            if !((count > 0 && removed_count == count)
                                || removed_count == hole.len())
                                && hole[removed_count] == key_idx
                            {
                                txn.del(cfs.data_cf.clone(), kv.0)?;
                                removed_count += 1;
                                continue;
                            }

                            // check if key idx need to be forward move
                            if removed_count > 0 {
                                let new_data_key = self.inner_db.key_encoder.encode_list_data_key(
                                    &key,
                                    key_idx + removed_count as u64,
                                    version,
                                );
                                txn.put(cfs.data_cf.clone(), new_data_key, kv.1)?;
                                txn.del(cfs.data_cf.clone(), kv.0)?;
                            }
                        }

                        // update meta key or delete it if no element left
                        if len == removed_count as u64 {
                            txn.del(cfs.meta_cf.clone(), meta_key)?;
                        } else {
                            let new_meta_value = self.inner_db.key_encoder.encode_list_meta_value(
                                ttl,
                                version,
                                left + removed_count as u64,
                                right,
                            );
                            txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;
                        }
                    }
                    Ok(removed_count as i64)
                }
                None => Ok(0),
            }
        });

        match resp {
            Ok(_) => Ok(resp_ok()),
            Err(e) => Ok(resp_err(e)),
        }
    }
}

impl TxnCommand for ListCommand<'_> {
    fn txn_del(&self, txn: &RocksTransaction, key: &str) -> RocksResult<()> {
        let key = key.to_owned();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        let cfs = ListCF::new(&self.inner_db.client);

        match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
            Some(meta_value) => {
                let (_, version, left, right) = KeyDecoder::decode_key_list_meta(&meta_value);
                let len = right - left;

                if len >= async_del_list_threshold_or_default() as u64 {
                    // async delete
                    // delete meta key and create gc key and gc version key with the version
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
                        vec![self.inner_db.key_encoder.get_type_bytes(DataType::List)],
                    )?;
                } else {
                    let bound_range = self
                        .inner_db
                        .key_encoder
                        .encode_list_data_key_range(&key, version);
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

    fn txn_expire_if_needed(&self, txn: &RocksTransaction, key: &str) -> RocksResult<i64> {
        let key = key.to_owned();
        let meta_key = self.inner_db.key_encoder.encode_meta_key(&key);
        let cfs = ListCF::new(&self.inner_db.client);

        match txn.get(cfs.meta_cf.clone(), meta_key.clone())? {
            Some(meta_value) => {
                let (ttl, version, left, right) = KeyDecoder::decode_key_list_meta(&meta_value);
                if !key_is_expired(ttl) {
                    return Ok(0);
                }

                let len = right - left;
                if len >= async_expire_list_threshold_or_default() as u64 {
                    // async delete
                    // delete meta key and create gc key and gc version key with the version
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
                        vec![self.inner_db.key_encoder.get_type_bytes(DataType::List)],
                    )?;
                } else {
                    let bound_range = self
                        .inner_db
                        .key_encoder
                        .encode_list_data_key_range(&key, version);
                    let iter = txn.scan_keys(cfs.data_cf.clone(), bound_range, u32::MAX)?;

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
        let cfs = ListCF::new(&self.inner_db.client);
        let meta_key = self.inner_db.key_encoder.encode_meta_key(key);
        let ttl = KeyDecoder::decode_key_ttl(meta_value);
        if key_is_expired(ttl) {
            self.txn_expire_if_needed(txn, key)?;
            return Ok(0);
        }
        let (_, version, left, right) = KeyDecoder::decode_key_list_meta(meta_value);
        let new_meta_value = self
            .inner_db
            .key_encoder
            .encode_list_meta_value(timestamp, version, left, right);
        txn.put(cfs.meta_cf.clone(), meta_key, new_meta_value)?;
        Ok(1)
    }

    fn txn_gc(&self, txn: &RocksTransaction, key: &str, version: u16) -> RocksResult<()> {
        let cfs = ListCF::new(&self.inner_db.client);
        // delete all data key of this key and version
        let bound_range = self
            .inner_db
            .key_encoder
            .encode_list_data_key_range(key, version);
        let iter = txn.scan_keys(cfs.data_cf.clone(), bound_range, u32::MAX)?;
        for k in iter {
            txn.del(cfs.data_cf.clone(), k)?;
        }
        Ok(())
    }
}
