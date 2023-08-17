use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Zpop {
    key: String,
    count: i64,
    valid: bool,
}

impl Zpop {
    pub fn new(key: impl ToString, count: i64) -> Zpop {
        Zpop {
            key: key.to_string(),
            count,
            valid: true,
        }
    }

    pub async fn execute(&mut self, inner_db: &DBInner, from_min: bool) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(inner_db)
            .zpop(&self.key, from_min, self.count as u64)
            .await
    }
}

impl Invalid for Zpop {
    fn new_invalid() -> Zpop {
        Zpop {
            key: "".to_string(),
            count: 0,
            valid: false,
        }
    }
}
