use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Bound;

#[derive(Serialize, Deserialize, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Key(pub(super) Vec<u8>);

impl Key {
    pub const EMPTY: Self = Key(Vec::new());

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(super) fn zero_terminated(&self) -> bool {
        self.0.last().map(|i| *i == 0).unwrap_or(false)
    }

    pub(super) fn push_zero(&mut self) {
        self.0.push(0)
    }

    pub(super) fn into_lower_bound(mut self) -> Bound<Key> {
        if self.zero_terminated() {
            self.0.pop().unwrap();
            Bound::Excluded(self)
        } else {
            Bound::Included(self)
        }
    }

    pub(super) fn into_upper_bound(mut self) -> Bound<Key> {
        if self.zero_terminated() {
            self.0.pop().unwrap();
            Bound::Included(self)
        } else {
            Bound::Excluded(self)
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl From<Vec<u8>> for Key {
    fn from(v: Vec<u8>) -> Self {
        Key(v)
    }
}

impl From<String> for Key {
    fn from(v: String) -> Key {
        Key(v.into_bytes())
    }
}

impl From<Key> for Vec<u8> {
    fn from(key: Key) -> Self {
        key.0
    }
}

impl<'a> From<&'a Key> for &'a [u8] {
    fn from(key: &'a Key) -> Self {
        &key.0
    }
}

impl<'a> From<&'a Vec<u8>> for &'a Key {
    fn from(key: &'a Vec<u8>) -> Self {
        unsafe { &*(key as *const Vec<u8> as *const Key) }
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<Key> for Vec<u8> {
    fn as_ref(&self) -> &Key {
        unsafe { &*(self as *const Vec<u8> as *const Key) }
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02X}")?;
        }
        Ok(())
    }
}
