use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Zcount {
    key: String,
    min: f64,
    min_inclusive: bool,
    max: f64,
    max_inclusive: bool,
    valid: bool,
}

impl Zcount {
    pub fn new(
        key: impl ToString,
        min: f64,
        min_inclusive: bool,
        max: f64,
        max_inclusive: bool,
    ) -> Zcount {
        Zcount {
            key: key.to_string(),
            min,
            min_inclusive,
            max,
            max_inclusive,
            valid: true,
        }
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(inner_db)
            .zcount(
                &self.key,
                self.min,
                self.min_inclusive,
                self.max,
                self.max_inclusive,
            )
            .await
    }
}

impl Invalid for Zcount {
    fn new_invalid() -> Zcount {
        Zcount {
            key: "".to_string(),
            min: 0f64,
            min_inclusive: false,
            max: 0f64,
            max_inclusive: false,
            valid: false,
        }
    }
}
