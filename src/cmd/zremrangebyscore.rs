use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Zremrangebyscore {
    key: String,
    min: f64,
    max: f64,
    valid: bool,
}

impl Zremrangebyscore {
    pub fn new(key: impl ToString, min: f64, max: f64) -> Zremrangebyscore {
        Zremrangebyscore {
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
            .zremrange_by_score(&self.key, self.min, self.max)
            .await
    }
}

impl Invalid for Zremrangebyscore {
    fn new_invalid() -> Zremrangebyscore {
        Zremrangebyscore {
            key: "".to_string(),
            min: 0f64,
            max: 0f64,
            valid: false,
        }
    }
}
