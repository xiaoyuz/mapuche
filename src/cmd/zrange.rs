use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Zrange {
    key: String,
    min: i64,
    max: i64,
    withscores: bool,
    reverse: bool,
    valid: bool,
}

impl Zrange {
    pub fn new(key: impl ToString, min: i64, max: i64, withscores: bool, reverse: bool) -> Zrange {
        Zrange {
            key: key.to_string(),
            min,
            max,
            withscores,
            reverse,
            valid: true,
        }
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(inner_db)
            .zrange(&self.key, self.min, self.max, self.withscores, self.reverse)
            .await
    }
}

impl Invalid for Zrange {
    fn new_invalid() -> Zrange {
        Zrange {
            key: "".to_string(),
            min: 0,
            max: 0,
            withscores: false,
            reverse: false,
            valid: false,
        }
    }
}
