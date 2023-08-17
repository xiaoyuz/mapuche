use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::list::ListCommand;
use bytes::Bytes;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Lset {
    key: String,
    idx: i64,
    element: Bytes,
    valid: bool,
}

impl Lset {
    pub fn new(key: impl ToString, idx: i64, element: impl ToString) -> Lset {
        Lset {
            key: key.to_string(),
            idx,
            element: element.to_string().into(),
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
        ListCommand::new(inner_db)
            .lset(&self.key, self.idx, &self.element)
            .await
    }
}

impl Invalid for Lset {
    fn new_invalid() -> Lset {
        Lset {
            key: "".to_owned(),
            idx: 0,
            element: Bytes::new(),
            valid: false,
        }
    }
}
