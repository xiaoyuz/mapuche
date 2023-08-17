use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::string::StringCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Scan {
    start: String,
    count: i64,
    regex: String,
    valid: bool,
}

impl Scan {
    pub fn new(start: impl ToString, count: i64, regex: impl ToString) -> Scan {
        Scan {
            start: start.to_string(),
            count,
            regex: regex.to_string(),
            valid: true,
        }
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand::new(inner_db)
            .scan(&self.start, self.count.try_into().unwrap(), &self.regex)
            .await
    }
}

impl Invalid for Scan {
    fn new_invalid() -> Scan {
        Scan {
            start: "".to_owned(),
            count: 0,
            regex: "".to_owned(),
            valid: false,
        }
    }
}
