use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::hash::HashCommand;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Hget {
    key: String,
    field: String,
    valid: bool,
}

impl Hget {
    pub fn new(key: impl ToString, field: impl ToString) -> Hget {
        Hget {
            field: field.to_string(),
            key: key.to_string(),
            valid: true,
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
            .hget(&self.key, &self.field)
            .await
    }
}

impl Invalid for Hget {
    fn new_invalid() -> Hget {
        Hget {
            field: "".to_owned(),
            key: "".to_owned(),
            valid: false,
        }
    }
}
