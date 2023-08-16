use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::set::SetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Srem {
    key: String,
    members: Vec<String>,
    valid: bool,
}

impl Srem {
    pub fn new(key: &str) -> Srem {
        Srem {
            key: key.to_string(),
            members: vec![],
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
        SetCommand::new(inner_db)
            .srem(&self.key, &self.members)
            .await
    }
}

impl Invalid for Srem {
    fn new_invalid() -> Srem {
        Srem {
            key: "".to_string(),
            members: vec![],
            valid: false,
        }
    }
}
