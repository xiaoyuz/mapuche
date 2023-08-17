use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::hash::HashCommand;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Hstrlen {
    key: String,
    field: String,
    valid: bool,
}

impl Hstrlen {
    pub fn new(key: impl ToString, field: impl ToString) -> Hstrlen {
        Hstrlen {
            field: field.to_string(),
            key: key.to_string(),
            valid: true,
        }
    }

    pub fn new_invalid() -> Hstrlen {
        Hstrlen {
            field: "".to_owned(),
            key: "".to_owned(),
            valid: false,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub async fn execute(&self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        HashCommand::new(inner_db)
            .hstrlen(&self.key, &self.field)
            .await
    }
}

impl Invalid for Hstrlen {
    fn new_invalid() -> Hstrlen {
        Hstrlen {
            field: "".to_owned(),
            key: "".to_owned(),
            valid: false,
        }
    }
}
