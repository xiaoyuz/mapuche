use crate::rocks::kv::key::Key;
use crate::rocks::kv::value::Value;
use serde::{Deserialize, Serialize};
use std::{fmt, str};

#[derive(Serialize, Deserialize, Default, Clone, Eq, PartialEq)]
pub struct KvPair(pub Key, pub Value);

impl KvPair {
    pub fn new(key: impl Into<Key>, value: impl Into<Value>) -> Self {
        KvPair(key.into(), value.into())
    }

    pub fn key(&self) -> &Key {
        &self.0
    }

    pub fn value(&self) -> &Value {
        &self.1
    }

    pub fn into_key(self) -> Key {
        self.0
    }

    pub fn into_value(self) -> Value {
        self.1
    }

    pub fn key_mut(&mut self) -> &mut Key {
        &mut self.0
    }

    pub fn value_mut(&mut self) -> &mut Value {
        &mut self.1
    }

    pub fn set_key(&mut self, k: impl Into<Key>) {
        self.0 = k.into();
    }

    pub fn set_value(&mut self, v: impl Into<Value>) {
        self.1 = v.into();
    }
}

impl<K, V> From<(K, V)> for KvPair
where
    K: Into<Key>,
    V: Into<Value>,
{
    fn from((k, v): (K, V)) -> Self {
        KvPair(k.into(), v.into())
    }
}

impl From<KvPair> for (Key, Value) {
    fn from(pair: KvPair) -> Self {
        (pair.0, pair.1)
    }
}

impl From<KvPair> for Key {
    fn from(pair: KvPair) -> Self {
        pair.0
    }
}

impl AsRef<Key> for KvPair {
    fn as_ref(&self) -> &Key {
        &self.0
    }
}

impl AsRef<Value> for KvPair {
    fn as_ref(&self) -> &Value {
        &self.1
    }
}

impl fmt::Debug for KvPair {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let KvPair(key, value) = self;
        match str::from_utf8(value) {
            Ok(s) => write!(f, "KvPair({:?}, {:?})", &key, s),
            Err(_) => write!(f, "KvPair({:?}, {:?})", &key, &value),
        }
    }
}
