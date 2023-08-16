use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::string::StringCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handles string values.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Get {
    /// Name of the key to get
    key: String,

    valid: bool,
}

impl Get {
    /// Create a new `Get` command which fetches `key`.
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn execute(&self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand::new(inner_db).get(&self.key).await
    }
}

impl Invalid for Get {
    fn new_invalid() -> Get {
        Get {
            key: "".to_owned(),
            valid: false,
        }
    }
}
