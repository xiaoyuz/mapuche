use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::list::ListCommand;
use bytes::Bytes;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Linsert {
    key: String,
    before_pivot: bool,
    pivot: Bytes,
    element: Bytes,
    valid: bool,
}

impl Linsert {
    pub fn new(
        key: impl ToString,
        before_pivot: bool,
        pivot: impl ToString,
        element: impl ToString,
    ) -> Linsert {
        Linsert {
            key: key.to_string(),
            before_pivot,
            pivot: pivot.to_string().into(),
            element: element.to_string().into(),
            valid: true,
        }
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ListCommand::new(inner_db)
            .linsert(&self.key, self.before_pivot, &self.pivot, &self.element)
            .await
    }
}

impl Invalid for Linsert {
    fn new_invalid() -> Linsert {
        Linsert {
            key: "".to_owned(),
            before_pivot: false,
            pivot: Bytes::new(),
            element: Bytes::new(),
            valid: false,
        }
    }
}
