use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::list::ListCommand;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Ltrim {
    key: String,
    start: i64,
    end: i64,
    valid: bool,
}

impl Ltrim {
    pub fn new(key: impl ToString, start: i64, end: i64) -> Ltrim {
        Ltrim {
            key: key.to_string(),
            start,
            end,
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ListCommand::new(inner_db)
            .ltrim(&self.key, self.start, self.end)
            .await
    }
}

impl Invalid for Ltrim {
    fn new_invalid() -> Ltrim {
        Ltrim {
            key: "".to_owned(),
            start: 0,
            end: 0,
            valid: false,
        }
    }
}
