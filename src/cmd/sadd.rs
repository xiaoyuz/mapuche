use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::set::SetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Sadd {
    key: String,
    members: Vec<String>,
    valid: bool,
}

impl Sadd {
    pub fn new(key: impl ToString, members: &[impl ToString]) -> Sadd {
        Sadd {
            key: key.to_string(),
            members: members.iter().map(|it| it.to_string()).collect(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        SetCommand::new(inner_db)
            .sadd(&self.key, &self.members)
            .await
    }
}

impl Invalid for Sadd {
    fn new_invalid() -> Sadd {
        Sadd {
            key: "".to_string(),
            members: vec![],
            valid: false,
        }
    }
}
