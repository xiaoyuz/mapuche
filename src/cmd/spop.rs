use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::set::SetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Spop {
    key: String,
    count: i64,
    valid: bool,
}

impl Spop {
    pub fn new(key: impl ToString, count: i64) -> Spop {
        Spop {
            key: key.to_string(),
            count,
            valid: true,
        }
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        SetCommand::new(inner_db)
            .spop(&self.key, self.count as u64)
            .await
    }
}

impl Invalid for Spop {
    fn new_invalid() -> Spop {
        Spop {
            key: "".to_string(),
            count: 0,
            valid: false,
        }
    }
}
