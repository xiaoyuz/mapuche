use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Zrem {
    key: String,
    members: Vec<String>,
    valid: bool,
}

impl Zrem {
    pub fn new(key: impl ToString, members: &[impl ToString]) -> Zrem {
        Zrem {
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
        ZsetCommand::new(inner_db)
            .zrem(&self.key, &self.members)
            .await
    }
}

impl Invalid for Zrem {
    fn new_invalid() -> Zrem {
        Zrem {
            key: "".to_string(),
            members: vec![],
            valid: false,
        }
    }
}
