use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::hash::HashCommand;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Hlen {
    key: String,
    valid: bool,
}

impl Hlen {
    pub fn new(key: impl ToString) -> Hlen {
        Hlen {
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
        HashCommand::new(inner_db).hlen(&self.key).await
    }
}

impl Invalid for Hlen {
    fn new_invalid() -> Hlen {
        Hlen {
            key: "".to_string(),
            valid: false,
        }
    }
}
