use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::list::ListCommand;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Lrange {
    key: String,
    left: i64,
    right: i64,
    valid: bool,
}

impl Lrange {
    pub fn new(key: impl ToString, left: i64, right: i64) -> Lrange {
        Lrange {
            key: key.to_string(),
            left,
            right,
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ListCommand::new(inner_db)
            .lrange(&self.key, self.left, self.right)
            .await
    }
}

impl Invalid for Lrange {
    fn new_invalid() -> Lrange {
        Lrange {
            key: "".to_owned(),
            left: 0,
            right: 0,
            valid: false,
        }
    }
}
