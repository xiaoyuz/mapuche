use crate::cmd::Invalid;
use crate::db::DBInner;

use crate::rocks::errors::DECREMENT_OVERFLOW;
use crate::Frame;

use serde::{Deserialize, Serialize};

use crate::rocks::string::StringCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::{resp_err, resp_invalid_arguments};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IncrDecr {
    key: String,
    step: i64,
    valid: bool,
}

impl IncrDecr {
    pub fn new(key: impl ToString, step: i64) -> IncrDecr {
        IncrDecr {
            key: key.to_string(),
            step,
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn execute(&mut self, inner_db: &DBInner, inc: bool) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }

        if !inc {
            if self.step == i64::MIN {
                return Ok(resp_err(DECREMENT_OVERFLOW));
            }
            self.step = -self.step;
        }
        StringCommand::new(inner_db)
            .incr(&self.key, self.step)
            .await
    }
}

impl Invalid for IncrDecr {
    fn new_invalid() -> IncrDecr {
        IncrDecr {
            key: "".to_owned(),
            step: 0,
            valid: false,
        }
    }
}
