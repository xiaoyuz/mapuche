use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::set::SetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Srandmember {
    key: String,
    count: Option<i64>,
    valid: bool,
}

impl Srandmember {
    pub fn new(key: impl ToString, count: Option<i64>) -> Srandmember {
        Srandmember {
            key: key.to_string(),
            count,
            valid: true,
        }
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        let mut count;
        let repeatable;
        let array_resp;
        if self.count.is_none() {
            repeatable = false;
            count = 1;
            array_resp = false;
        } else {
            array_resp = true;
            count = self.count.unwrap();
            if count > 0 {
                repeatable = false;
            } else {
                repeatable = true;
                count = -count;
            }
        }
        SetCommand::new(inner_db)
            .srandmemeber(&self.key, count, repeatable, array_resp)
            .await
    }
}

impl Invalid for Srandmember {
    fn new_invalid() -> Srandmember {
        Srandmember {
            key: "".to_string(),
            count: None,
            valid: false,
        }
    }
}
