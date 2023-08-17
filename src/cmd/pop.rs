use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::list::ListCommand;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Pop {
    key: String,
    count: i64,
    valid: bool,
}

impl Pop {
    pub fn new(key: impl ToString, count: i64) -> Pop {
        Pop {
            key: key.to_string(),
            count,
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn execute(&mut self, inner_db: &DBInner, op_left: bool) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ListCommand::new(inner_db)
            .pop(&self.key, op_left, self.count)
            .await
    }
}

impl Invalid for Pop {
    fn new_invalid() -> Pop {
        Pop {
            key: "".to_owned(),
            count: 0,
            valid: false,
        }
    }
}
