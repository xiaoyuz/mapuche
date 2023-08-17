use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::list::ListCommand;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Llen {
    key: String,
    valid: bool,
}

impl Llen {
    pub fn new(key: impl ToString) -> Llen {
        Llen {
            key: key.to_string(),
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
        ListCommand::new(inner_db).llen(&self.key).await
    }
}

impl Invalid for Llen {
    fn new_invalid() -> Llen {
        Llen {
            key: "".to_owned(),
            valid: false,
        }
    }
}
