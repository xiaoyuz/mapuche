use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Zrank {
    key: String,
    member: String,
    valid: bool,
}

impl Zrank {
    pub fn new(key: impl ToString, member: impl ToString) -> Zrank {
        Zrank {
            key: key.to_string(),
            member: member.to_string(),
            valid: true,
        }
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(inner_db)
            .zrank(&self.key, &self.member)
            .await
    }
}

impl Invalid for Zrank {
    fn new_invalid() -> Zrank {
        Zrank {
            key: "".to_string(),
            member: "".to_string(),
            valid: false,
        }
    }
}
