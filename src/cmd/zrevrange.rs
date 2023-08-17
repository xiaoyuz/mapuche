use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Zrevrange {
    key: String,
    min: i64,
    max: i64,
    withscores: bool,
    valid: bool,
}

impl Zrevrange {
    pub fn new(key: impl ToString, min: i64, max: i64, withscores: bool) -> Zrevrange {
        Zrevrange {
            key: key.to_string(),
            min,
            max,
            withscores,
            valid: true,
        }
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(inner_db)
            .zrange(&self.key, self.min, self.max, self.withscores, true)
            .await
    }
}

impl Invalid for Zrevrange {
    fn new_invalid() -> Zrevrange {
        Zrevrange {
            key: "".to_string(),
            min: 0,
            max: 0,
            withscores: false,
            valid: false,
        }
    }
}
