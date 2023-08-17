use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::list::ListCommand;
use bytes::Bytes;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Lrem {
    key: String,
    count: i64,
    element: Bytes,
    valid: bool,
}

impl Lrem {
    pub fn new(key: impl ToString, count: i64, element: impl ToString) -> Lrem {
        Lrem {
            key: key.to_string(),
            count,
            element: element.to_string().into(),
            valid: true,
        }
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        let mut from_head = true;
        let mut count = self.count;
        if self.count < 0 {
            from_head = false;
            count = -count;
        }
        ListCommand::new(inner_db)
            .lrem(&self.key, count as usize, from_head, &self.element)
            .await
    }
}

impl Invalid for Lrem {
    fn new_invalid() -> Lrem {
        Lrem {
            key: "".to_owned(),
            count: 0,
            element: Bytes::new(),
            valid: false,
        }
    }
}
