use crate::cmd::Invalid;
use crate::db::DBInner;

use crate::Frame;

use serde::{Deserialize, Serialize};

use crate::rocks::string::StringCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Type {
    key: String,
    valid: bool,
}

impl Type {
    pub fn new(key: impl ToString) -> Type {
        Type {
            key: key.to_string(),
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn execute(&self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand::new(inner_db).get_type(&self.key).await
    }
}

impl Invalid for Type {
    fn new_invalid() -> Type {
        Type {
            key: "".to_owned(),
            valid: false,
        }
    }
}
