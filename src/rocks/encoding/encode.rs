use std::ops::Range;
use crate::config::config_meta_key_number_or_default;
use crate::rocks::encoding::{DataType, ENC_ASC_PADDING, ENC_GROUP_SIZE, ENC_MARKER};
use crate::rocks::get_instance_id;
use crate::rocks::kv::bound_range::BoundRange;
use crate::rocks::kv::key::Key;
use crate::rocks::kv::value::Value;

pub struct KeyEncoder {
    // instance_id will be encoded to 2 bytes vec
    instance_id: [u8; 2],
    // meta_key_number is the number of sub meta key of a new key
    meta_key_number: u16,
}

pub const RAW_KEY_PREFIX: u8 = b'r';
pub const TXN_KEY_PREFIX: u8 = b'x';

pub const DATA_TYPE_USER: u8 = b'u';
pub const DATA_TYPE_USER_END: u8 = b'v';
pub const DATA_TYPE_TOPO: u8 = b't';
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

    pub fn encode_bytes(&self, key: &[u8]) -> Vec<u8> {
        let len = key.len();
        let mut index = 0;
        let mut enc = vec![];
        while index <= len {
            let remain = len - index;
            let mut pad: usize = 0;
            if remain > ENC_GROUP_SIZE {
                enc.extend_from_slice(&key[index..index + ENC_GROUP_SIZE]);
            } else {
                pad = ENC_GROUP_SIZE - remain;
                enc.extend_from_slice(&key[index..]);
                enc.extend_from_slice(&ENC_ASC_PADDING[..pad]);
            }
            enc.push(ENC_MARKER - pad as u8);
            index += ENC_GROUP_SIZE;
        }
        enc
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

    pub fn encode_raw_kv_string(&self, ukey: &str) -> Key {
        let mut key = Vec::with_capacity(4 + ukey.len());
        key.push(RAW_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_META);
        key.extend_from_slice(ukey.as_bytes());
        key.into()
    }

    pub fn encode_txn_kv_string(&self, ukey: &str) -> Key {
        let enc_ukey = self.encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(5 + enc_ukey.len());

        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_USER);
        key.extend_from_slice(&enc_ukey);
        key.push(DATA_TYPE_META);
        key.into()
    }

    fn encode_txn_kv_string_internal(&self, vsize: usize, ttl: u64, version: u16) -> Value {
        let dt = self.get_type_bytes(DataType::String);
        let mut val = Vec::with_capacity(11 + vsize);
        val.push(dt);
        val.extend_from_slice(&ttl.to_be_bytes());
        val.extend_from_slice(&version.to_be_bytes());
        val
    }

    pub fn encode_txn_kv_string_slice(&self, value: &[u8], ttl: u64) -> Value {
        let mut val = self.encode_txn_kv_string_internal(value.len(), ttl, 0);
        val.extend_from_slice(value);
        val
    }

    pub fn encode_txn_kv_string_value(&self, value: &mut Value, ttl: u64) -> Value {
        let mut val = self.encode_txn_kv_string_internal(value.len(), ttl, 0);
        val.append(value);
        val
    }

    pub fn encode_raw_kv_strings(&self, keys: &[String]) -> Vec<Key> {
        keys.iter()
            .map(|ukey| self.encode_raw_kv_string(ukey))
            .collect()
    }

    pub fn encode_txn_kv_strings(&self, keys: &[String]) -> Vec<Key> {
        keys.iter()
            .map(|ukey| self.encode_txn_kv_string(ukey))
            .collect()
    }

    fn encode_txn_kv_meta_common_prefix(&self, enc_ukey: &[u8], key: &mut Vec<u8>) {
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_USER);
        key.extend_from_slice(enc_ukey);
        key.push(DATA_TYPE_META);
    }

    pub fn encode_txn_kv_meta_key(&self, ukey: &str) -> Key {
        let enc_ukey = self.encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(5 + enc_ukey.len());

        self.encode_txn_kv_meta_common_prefix(&enc_ukey, &mut key);
        key.into()
    }

    pub fn encode_txn_kv_keyspace_end(&self) -> Key {
        let mut key = Vec::with_capacity(4);
        key.push(TXN_KEY_PREFIX);
        key.extend_from_slice(self.instance_id.as_slice());
        key.push(DATA_TYPE_USER_END);
        key.into()
    }

    pub fn encode_txn_kv_sub_meta_key(&self, ukey: &str, version: u16, idx: u16) -> Key {
        let enc_ukey = self.encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(10 + enc_ukey.len());

        self.encode_txn_kv_meta_common_prefix(&enc_ukey, &mut key);

        key.extend_from_slice(&version.to_be_bytes());
        key.push(PLACE_HOLDER);
        key.extend_from_slice(&idx.to_be_bytes());
        key.into()
    }

    pub fn encode_txn_kv_sub_meta_key_start(&self, ukey: &str, version: u16) -> Key {
        let enc_ukey = self.encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + enc_ukey.len());

        self.encode_txn_kv_meta_common_prefix(&enc_ukey, &mut key);

        key.extend_from_slice(&version.to_be_bytes());
        key.push(PLACE_HOLDER);
        key.into()
    }

    pub fn encode_txn_kv_sub_meta_key_end(&self, ukey: &str, version: u16) -> Key {
        let enc_ukey = self.encode_bytes(ukey.as_bytes());
        let mut key = Vec::with_capacity(8 + ukey.len());

        self.encode_txn_kv_meta_common_prefix(&enc_ukey, &mut key);

        key.extend_from_slice(&version.to_be_bytes());
        key.push(PLACE_HOLDER + 1);
        key.into()
    }

    pub fn encode_txn_kv_sub_meta_key_range(&self, key: &str, version: u16) -> BoundRange {
        let sub_meta_key_start = self.encode_txn_kv_sub_meta_key_start(key, version);
        let sub_meta_key_end = self.encode_txn_kv_sub_meta_key_end(key, version);
        let range: Range<Key> = sub_meta_key_start..sub_meta_key_end;
        range.into()
    }
}