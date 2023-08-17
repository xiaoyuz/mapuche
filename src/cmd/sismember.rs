use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::set::SetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Sismember {
    key: String,
    member: String,
    valid: bool,
}

impl Sismember {
    pub fn new(key: impl ToString, member: impl ToString) -> Sismember {
        Sismember {
            key: key.to_string(),
            member: member.to_string(),
            valid: true,
        }
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        let mut members = vec![];
        members.push(self.member.clone());
        SetCommand::new(inner_db)
            .sismember(&self.key, &members, false)
            .await
    }
}

impl Invalid for Sismember {
    fn new_invalid() -> Sismember {
        Sismember {
            key: "".to_string(),
            member: "".to_string(),
            valid: false,
        }
    }
}
