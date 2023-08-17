use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::hash::HashCommand;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Hmget {
    key: String,
    fields: Vec<String>,
    valid: bool,
}

impl Hmget {
    pub fn new(key: impl ToString, fields: &[impl ToString]) -> Hmget {
        Hmget {
            key: key.to_string(),
            fields: fields.iter().map(|it| it.to_string()).collect(),
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn fields(&self) -> &Vec<String> {
        &self.fields
    }

    pub async fn execute(&self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        HashCommand::new(inner_db)
            .hmget(&self.key, &self.fields)
            .await
    }
}

impl Invalid for Hmget {
    fn new_invalid() -> Hmget {
        Hmget {
            key: "".to_owned(),
            fields: vec![],
            valid: false,
        }
    }
}
