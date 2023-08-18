use crate::config::config_meta_key_number_or_default;
use crate::rocks::encoding::{DataType, SIGN_MASK};
use crate::rocks::get_instance_id;
use crate::rocks::kv::bound_range::BoundRange;
use crate::rocks::kv::key::Key;
use crate::rocks::kv::value::Value;
use std::ops::{Range, RangeInclusive};

use super::encode_bytes;

pub struct KeyEncoder {
    // instance_id will be encoded to 2 bytes vec
    instance_id: [u8; 2],
    // meta_key_number is the number of sub meta key of a new key
    meta_key_number: u16,
}

pub const TXN_KEY_PREFIX: u8 = b'x';

pub const DATA_TYPE_USER: u8 = b'u';
pub const DATA_TYPE_USER_END: u8 = b'v';
pub const DATA_TYPE_GC: u8 = b'g';
pub const DATA_TYPE_GC_VERSION: u8 = b'v';

pub const DATA_TYPE_META: u8 = b'm';
pub const DATA_TYPE_SCORE: u8 = b'S';
pub const DATA_TYPE_HASH: u8 = b'h';
pub const DATA_TYPE_LIST: u8 = b'l';
pub const DATA_TYPE_SET: u8 = b's';
pub const DATA_TYPE_ZSET: u8 = b'z';

pub const PLACE_HOLDER: u8 = b'`';

impl KeyEncoder {
    pub fn new() -> Self {
        KeyEncoder {
            instance_id: u16::try_from(get_instance_id()).unwrap().to_be_bytes(),
            meta_key_number: config_meta_key_number_or_default(),
        }
    }

    pub fn get_type_bytes(&self, dt: DataType) -> u8 {
        match dt {
            DataType::String => 0,
            DataType::Hash => 1,
            DataType::List => 2,
            DataType::Set => 3,
            DataType::Zset => 4,
            DataType::Null => 5,
        }
    }

    pub fn encode_string(&self, ukey: &str) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(5 + enc_ukey.len());

        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_USER);
        key.extend_from_slice(&enc_ukey);
        key.push(DATA_TYPE_META);
        key.into()
    }

    fn encode_string_internal(&self, vsize: usize, ttl: i64, version: u16) -> Value {
        let dt = self.get_type_bytes(DataType::String);
        let mut val = Vec::with_capacity(11 + vsize);
        val.push(dt);
        val.extend_from_slice(&ttl.to_be_bytes());
        val.extend_from_slice(&version.to_be_bytes());
        val
    }

    pub fn encode_string_slice(&self, value: &[u8], ttl: i64) -> Value {
        let mut val = self.encode_string_internal(value.len(), ttl, 0);
        val.extend_from_slice(value);
        val
    }

    pub fn encode_string_value(&self, value: &mut Value, ttl: i64) -> Value {
        let mut val = self.encode_string_internal(value.len(), ttl, 0);
        val.append(value);
        val
    }

    pub fn encode_strings(&self, keys: &[String]) -> Vec<Key> {
        keys.iter().map(|ukey| self.encode_string(ukey)).collect()
    }

    fn encode_meta_common_prefix(&self, enc_ukey: &[u8], key: &mut Vec<u8>) {
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_USER);
        key.extend_from_slice(enc_ukey);
        key.push(DATA_TYPE_META);
    }

    pub fn encode_meta_key(&self, ukey: &str) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(5 + enc_ukey.len());

        self.encode_meta_common_prefix(&enc_ukey, &mut key);
        key.into()
    }

    pub fn encode_keyspace_end(&self) -> Key {
        let mut key = Vec::with_capacity(4);
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_USER_END);
        key.into()
    }

    pub fn encode_sub_meta_key(&self, ukey: &str, version: u16, idx: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(10 + enc_ukey.len());

        self.encode_meta_common_prefix(&enc_ukey, &mut key);

        key.extend_from_slice(&version.to_be_bytes());
        key.push(PLACE_HOLDER);
        key.extend_from_slice(&idx.to_be_bytes());
        key.into()
    }

    pub fn encode_sub_meta_key_start(&self, ukey: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len());

        self.encode_meta_common_prefix(&enc_ukey, &mut key);

        key.extend_from_slice(&version.to_be_bytes());
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_sub_meta_key_end(&self, ukey: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + ukey.len());

        self.encode_meta_common_prefix(&enc_ukey, &mut key);

        key.extend_from_slice(&version.to_be_bytes());
        key.push(PLACE_HOLDER + 1);
        key.into()
    }

    pub fn encode_sub_meta_key_range(&self, key: &str, version: u16) -> BoundRange {
        let sub_meta_key_start = self.encode_sub_meta_key_start(key, version);
        let sub_meta_key_end = self.encode_sub_meta_key_end(key, version);
        let range: Range<Key> = sub_meta_key_start..sub_meta_key_end;
        range.into()
    }

    pub fn encode_gc_key_prefix(&self, ukey: &str, data_type: u8, extra: usize) -> Vec<u8> {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(extra + enc_ukey.len());
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(data_type);
        key.push(PLACE_HOLDER);
        key.extend_from_slice(&enc_ukey);
        key
    }

    pub fn encode_gc_key(&self, ukey: &str) -> Key {
        self.encode_gc_key_prefix(ukey, DATA_TYPE_GC, 5).into()
    }

    pub fn encode_gc_version_key(&self, ukey: &str, version: u16) -> Key {
        let mut key = self.encode_gc_key_prefix(ukey, DATA_TYPE_GC_VERSION, 7);
        key.extend_from_slice(&version.to_be_bytes());
        key.into()
    }

    fn encode_type_data_key_prefix(
        &self,
        key_type: u8,
        enc_ukey: &[u8],
        key: &mut Vec<u8>,
        version: u16,
    ) {
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_USER);
        key.extend_from_slice(enc_ukey);
        key.push(key_type);
        key.extend_from_slice(&version.to_be_bytes());
    }

    pub fn encode_set_data_key_start(&self, ukey: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len());

        self.encode_type_data_key_prefix(DATA_TYPE_SET, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_set_data_key_end(&self, ukey: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len());

        self.encode_type_data_key_prefix(DATA_TYPE_SET, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER + 1);
        key.into()
    }

    pub fn encode_set_data_key_range(&self, key: &str, version: u16) -> BoundRange {
        let data_key_start = self.encode_set_data_key_start(key, version);
        let data_key_end = self.encode_set_data_key_end(key, version);
        let range: Range<Key> = data_key_start..data_key_end;
        range.into()
    }

    pub fn encode_set_data_key(&self, ukey: &str, member: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len() + member.len());

        self.encode_type_data_key_prefix(DATA_TYPE_SET, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER);
        key.extend_from_slice(member.as_bytes());
        key.into()
    }

    pub fn encode_set_meta_value(&self, ttl: i64, version: u16, index_size: u16) -> Value {
        let dt = self.get_type_bytes(DataType::Set);
        let mut val = Vec::with_capacity(13);

        val.push(dt);
        val.extend_from_slice(&ttl.to_be_bytes());
        val.extend_from_slice(&version.to_be_bytes());
        // if index_size is 0 means this is a new created key, use the default config number
        if index_size == 0 {
            val.extend_from_slice(&self.meta_key_number.to_be_bytes());
        } else {
            val.extend_from_slice(&index_size.to_be_bytes());
        }
        val
    }

    fn encode_gc_version_key_bound(&self, start: bool) -> Key {
        let mut key = Vec::with_capacity(5);
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_GC_VERSION);
        if start {
            key.push(PLACE_HOLDER);
        } else {
            key.push(PLACE_HOLDER + 1);
        }
        key.into()
    }

    pub fn encode_gc_version_key_range(&self) -> BoundRange {
        let range_start = self.encode_gc_version_key_bound(true);
        let range_end = self.encode_gc_version_key_bound(false);
        let range: Range<Key> = range_start..range_end;
        range.into()
    }

    /// idx range [0, 1<<64]
    /// left initial value  1<<32, left is point to the left element
    /// right initial value 1<<32, right is point to the next right position of right element
    /// list is indicated as null if left index equal to right
    pub fn encode_list_data_key(&self, ukey: &str, idx: u64, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(16 + enc_ukey.len());

        self.encode_type_data_key_prefix(DATA_TYPE_LIST, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER);
        key.extend_from_slice(&idx.to_be_bytes());
        key.into()
    }

    pub fn encode_list_data_key_idx_range(
        &self,
        key: &str,
        start: u64,
        end: u64,
        version: u16,
    ) -> BoundRange {
        let data_key_start = self.encode_list_data_key(key, start, version);
        let data_key_end = self.encode_list_data_key(key, end, version);
        let range: RangeInclusive<Key> = data_key_start..=data_key_end;
        range.into()
    }

    fn encode_list_data_key_start(&self, ukey: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len());

        self.encode_type_data_key_prefix(DATA_TYPE_LIST, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER);
        key.into()
    }

    fn encode_list_data_key_end(&self, ukey: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len());

        self.encode_type_data_key_prefix(DATA_TYPE_LIST, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER + 1);
        key.into()
    }

    pub fn encode_list_data_key_range(&self, key: &str, version: u16) -> BoundRange {
        let data_key_start = self.encode_list_data_key_start(key, version);
        let data_key_end = self.encode_list_data_key_end(key, version);
        let range: Range<Key> = data_key_start..data_key_end;
        range.into()
    }

    pub fn encode_list_meta_value(&self, ttl: i64, version: u16, left: u64, right: u64) -> Value {
        let dt = self.get_type_bytes(DataType::List);
        let mut val = Vec::with_capacity(27);

        val.push(dt);
        val.extend_from_slice(&ttl.to_be_bytes());
        val.extend_from_slice(&version.to_be_bytes());
        val.extend_from_slice(&left.to_be_bytes());
        val.extend_from_slice(&right.to_be_bytes());
        val
    }

    pub fn encode_hash_data_key(&self, ukey: &str, field: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len() + field.len());

        self.encode_type_data_key_prefix(DATA_TYPE_HASH, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER);
        key.extend_from_slice(field.as_bytes());
        key.into()
    }

    pub fn encode_hash_data_key_start(&self, ukey: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len());

        self.encode_type_data_key_prefix(DATA_TYPE_HASH, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_hash_data_key_end(&self, ukey: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len());

        self.encode_type_data_key_prefix(DATA_TYPE_HASH, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER + 1);
        key.into()
    }

    pub fn encode_hash_data_key_range(&self, key: &str, version: u16) -> BoundRange {
        let data_key_start = self.encode_hash_data_key_start(key, version);
        let data_key_end = self.encode_hash_data_key_end(key, version);
        let range: Range<Key> = data_key_start..data_key_end;
        range.into()
    }

    pub fn encode_hash_meta_value(&self, ttl: i64, version: u16, index_size: u16) -> Value {
        let dt = self.get_type_bytes(DataType::Hash);
        let mut val = Vec::with_capacity(13);

        val.push(dt);
        val.extend_from_slice(&ttl.to_be_bytes());
        val.extend_from_slice(&version.to_be_bytes());

        // if index_size is 0 means this is a new created key, use the default config number
        if index_size == 0 {
            val.extend_from_slice(&self.meta_key_number.to_be_bytes());
        } else {
            val.extend_from_slice(&index_size.to_be_bytes());
        }

        val
    }

    pub fn encode_zset_meta_value(&self, ttl: i64, version: u16, index_size: u16) -> Value {
        let dt = self.get_type_bytes(DataType::Zset);
        let mut val = Vec::with_capacity(13);

        val.push(dt);
        val.extend_from_slice(&ttl.to_be_bytes());
        val.extend_from_slice(&version.to_be_bytes());
        // if index_size is 0 means this is a new created key, use the default config number
        if index_size == 0 {
            val.extend_from_slice(&self.meta_key_number.to_be_bytes());
        } else {
            val.extend_from_slice(&index_size.to_be_bytes());
        }
        val
    }

    pub fn encode_zset_data_key(&self, ukey: &str, member: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len() + member.len());

        self.encode_type_data_key_prefix(DATA_TYPE_ZSET, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER);
        key.extend_from_slice(member.as_bytes());
        key.into()
    }

    pub fn encode_zset_data_key_start(&self, ukey: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len());

        self.encode_type_data_key_prefix(DATA_TYPE_ZSET, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_zset_data_key_end(&self, ukey: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len());

        self.encode_type_data_key_prefix(DATA_TYPE_ZSET, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER + 1);
        key.into()
    }

    pub fn encode_zset_data_key_range(&self, ukey: &str, version: u16) -> BoundRange {
        let data_key_start = self.encode_zset_data_key_start(ukey, version);
        let data_key_end = self.encode_zset_data_key_end(ukey, version);
        let range: Range<Key> = data_key_start..data_key_end;
        range.into()
    }

    pub fn encode_zset_data_value(&self, score: f64) -> Value {
        self.encode_f64_to_cmp_uint64(score).to_be_bytes().to_vec()
    }

    fn encode_f64_to_cmp_uint64(&self, score: f64) -> u64 {
        let mut b = score.to_bits();
        if score >= 0f64 {
            b |= SIGN_MASK;
        } else {
            b = !b;
        }

        b
    }

    // encode the member to score key
    pub fn encode_zset_score_key(&self, ukey: &str, score: f64, member: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(17 + enc_ukey.len() + member.len());
        let score = self.encode_f64_to_cmp_uint64(score);

        self.encode_type_data_key_prefix(DATA_TYPE_SCORE, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER);
        key.extend_from_slice(&score.to_be_bytes());
        key.push(PLACE_HOLDER);
        key.extend_from_slice(member.as_bytes());
        key.into()
    }

    pub fn encode_zset_score_key_start(&self, ukey: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len());

        self.encode_type_data_key_prefix(DATA_TYPE_SCORE, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_zset_score_key_end(&self, ukey: &str, version: u16) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len());

        self.encode_type_data_key_prefix(DATA_TYPE_SCORE, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER + 1);
        key.into()
    }

    pub fn encode_zset_score_key_range(&self, ukey: &str, version: u16) -> BoundRange {
        let range_start = self.encode_zset_score_key_start(ukey, version);
        let range_end = self.encode_zset_score_key_end(ukey, version);
        let range: Range<Key> = range_start..range_end;
        range.into()
    }

    pub fn encode_zset_score_key_score_start(
        &self,
        ukey: &str,
        score: f64,
        with_frontier: bool,
        version: u16,
    ) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(17 + enc_ukey.len());
        let mut score = self.encode_f64_to_cmp_uint64(score);
        if !with_frontier {
            score += 1;
        }

        self.encode_type_data_key_prefix(DATA_TYPE_SCORE, &enc_ukey, &mut key, version);

        key.push(PLACE_HOLDER);
        key.extend_from_slice(&score.to_be_bytes());
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_zset_score_key_score_end(
        &self,
        ukey: &str,
        score: f64,
        with_frontier: bool,
        version: u16,
    ) -> Key {
        let enc_ukey = encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(17 + enc_ukey.len());
        let mut score = self.encode_f64_to_cmp_uint64(score);
        if !with_frontier {
            score -= 1;
        }

        self.encode_type_data_key_prefix(DATA_TYPE_SCORE, &enc_ukey, &mut key, version);
        key.push(PLACE_HOLDER);
        key.extend_from_slice(&score.to_be_bytes());
        key.push(PLACE_HOLDER + 1);
        key.into()
    }
}

impl Default for KeyEncoder {
    fn default() -> Self {
        Self::new()
    }
}
