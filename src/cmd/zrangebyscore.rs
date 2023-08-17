use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Zrangebyscore {
    key: String,
    min: f64,
    min_inclusive: bool,
    max: f64,
    max_inclusive: bool,
    withscores: bool,
    valid: bool,
}

impl Zrangebyscore {
    pub fn new(
        key: impl ToString,
        min: f64,
        min_inclusive: bool,
        max: f64,
        max_inclusive: bool,
        withscores: bool,
    ) -> Zrangebyscore {
        Zrangebyscore {
            key: key.to_string(),
            min,
            min_inclusive,
            max,
            max_inclusive,
            withscores,
            valid: true,
        }
    }

    pub async fn execute(&mut self, inner_db: &DBInner, reverse: bool) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(inner_db)
            .zrange_by_score(
                &self.key,
                self.min,
                self.min_inclusive,
                self.max,
                self.max_inclusive,
                self.withscores,
                reverse,
            )
            .await
    }
}

impl Invalid for Zrangebyscore {
    fn new_invalid() -> Zrangebyscore {
        Zrangebyscore {
            key: "".to_string(),
            min: 0f64,
            min_inclusive: false,
            max: 0f64,
            max_inclusive: false,
            withscores: false,
            valid: false,
        }
    }
}
