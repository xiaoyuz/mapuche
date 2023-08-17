use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Zremrangebyrank {
    key: String,
    min: i64,
    max: i64,
    valid: bool,
}

impl Zremrangebyrank {
    pub fn new(key: impl ToString, min: i64, max: i64) -> Zremrangebyrank {
        Zremrangebyrank {
            key: key.to_string(),
            min,
            max,
            valid: true,
        }
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(inner_db)
            .zremrange_by_rank(&self.key, self.min, self.max)
            .await
    }
}

impl Invalid for Zremrangebyrank {
    fn new_invalid() -> Zremrangebyrank {
        Zremrangebyrank {
            key: "".to_string(),
            min: 0,
            max: 0,
            valid: false,
        }
    }
}
