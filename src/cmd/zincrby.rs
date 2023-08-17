use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Zincrby {
    key: String,
    step: f64,
    member: String,
    valid: bool,
}

impl Zincrby {
    pub fn new(key: impl ToString, step: f64, member: impl ToString) -> Zincrby {
        Zincrby {
            key: key.to_string(),
            step,
            member: member.to_string(),
            valid: true,
        }
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(inner_db)
            .zincrby(&self.key, self.step, &self.member)
            .await
    }
}

impl Invalid for Zincrby {
    fn new_invalid() -> Zincrby {
        Zincrby {
            key: "".to_string(),
            member: "".to_string(),
            step: 0f64,
            valid: false,
        }
    }
}
