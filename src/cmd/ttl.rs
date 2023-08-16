use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::string::StringCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TTL {
    key: String,
    valid: bool,
}

impl TTL {
    pub fn new(key: impl ToString) -> TTL {
        TTL {
            key: key.to_string(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn execute(&mut self, inner_db: &DBInner, is_millis: bool) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand::new(inner_db).ttl(&self.key, is_millis).await
    }
}

impl Invalid for TTL {
    fn new_invalid() -> TTL {
        TTL {
            key: "".to_owned(),
            valid: false,
        }
    }
}
