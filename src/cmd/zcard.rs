use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Zcard {
    key: String,
    valid: bool,
}

impl Zcard {
    pub fn new(key: impl ToString) -> Zcard {
        Zcard {
            key: key.to_string(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(inner_db).zcard(&self.key).await
    }
}

impl Invalid for Zcard {
    fn new_invalid() -> Zcard {
        Zcard {
            key: "".to_owned(),
            valid: false,
        }
    }
}
