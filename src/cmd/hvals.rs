use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::hash::HashCommand;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Hvals {
    key: String,
    valid: bool,
}

impl Hvals {
    pub fn new(key: impl ToString) -> Hvals {
        Hvals {
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
        HashCommand::new(inner_db)
            .hgetall(&self.key, false, true)
            .await
    }
}

impl Invalid for Hvals {
    fn new_invalid() -> Hvals {
        Hvals {
            key: "".to_owned(),
            valid: false,
        }
    }
}
