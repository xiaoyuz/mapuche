use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::list::ListCommand;
use bytes::Bytes;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Push {
    key: String,
    items: Vec<Bytes>,
    valid: bool,
}

impl Push {
    pub fn new(key: impl ToString, items: &[impl ToString]) -> Push {
        Push {
            items: items.iter().map(|it| it.to_string().into()).collect(),
            key: key.to_string(),
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn items(&self) -> &Vec<Bytes> {
        &self.items
    }

    pub async fn execute(&mut self, inner_db: &DBInner, op_left: bool) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ListCommand::new(inner_db)
            .push(&self.key, &self.items, op_left)
            .await
    }
}

impl Invalid for Push {
    fn new_invalid() -> Push {
        Push {
            items: vec![],
            key: "".to_owned(),
            valid: false,
        }
    }
}
