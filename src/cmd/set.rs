use crate::cmd::Invalid;
use crate::db::DBInner;
use crate::Frame;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::rocks::string::StringCommand;
use crate::utils::{resp_invalid_arguments, timestamp_from_ttl};

use crate::rocks::Result as RocksResult;

/// Set `key` to hold the string `value`.
///
/// If `key` already holds a value, it is overwritten, regardless of its type.
/// Any previous time to live associated with the key is discarded on successful
/// SET operation.
///
/// # Options
///
/// Currently, the following options are supported:
///
/// * EX `seconds` -- Set the specified expire time, in seconds.
/// * PX `milliseconds` -- Set the specified expire time, in milliseconds.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Set {
    /// the lookup key
    key: String,

    /// the value to be stored
    value: Bytes,

    /// When to expire the key
    expire: Option<i64>,

    /// Set if key is not present
    nx: Option<bool>,

    valid: bool,
}

impl Set {
    /// Create a new `Set` command which sets `key` to `value`.
    ///
    /// If `expire` is `Some`, the value should expire after the specified
    /// duration.
    pub fn new(
        key: impl ToString,
        value: impl ToString,
        expire: Option<i64>,
        nx: Option<bool>,
    ) -> Set {
        Set {
            key: key.to_string(),
            value: value.to_string().into(),
            expire,
            nx,
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the value
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// Get the expire
    pub fn expire(&self) -> Option<i64> {
        self.expire
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        match self.nx {
            Some(_) => self.put_not_exists(inner_db).await,
            None => self.put(inner_db).await,
        }
    }

    async fn put_not_exists(&self, inner_db: &DBInner) -> RocksResult<Frame> {
        StringCommand::new(inner_db)
            .put_not_exists(&self.key, &self.value)
            .await
    }

    async fn put(&self, inner_db: &DBInner) -> RocksResult<Frame> {
        let ttl = self.expire.map_or(-1, timestamp_from_ttl);
        StringCommand::new(inner_db)
            .put(&self.key, &self.value, ttl)
            .await
    }
}

impl Invalid for Set {
    fn new_invalid() -> Set {
        Set {
            key: "".to_owned(),
            value: Bytes::new(),
            expire: None,
            nx: None,
            valid: false,
        }
    }
}
