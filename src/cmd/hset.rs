use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::hash::HashCommand;
use crate::rocks::kv::kvpair::KvPair;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Hset {
    key: String,
    field_and_value: Vec<KvPair>,
    valid: bool,
}

impl Hset {
    pub fn new(key: impl ToString, field_and_value: &[(impl ToString, impl ToString)]) -> Hset {
        Hset {
            key: key.to_string(),
            field_and_value: field_and_value
                .iter()
                .map(|it| (it.0.to_string(), it.1.to_string()).into())
                .collect(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the field and value pairs
    pub fn fields(&self) -> &Vec<KvPair> {
        &self.field_and_value
    }

    pub async fn execute(
        &self,
        inner_db: &DBInner,
        is_hmset: bool,
        is_nx: bool,
    ) -> RocksResult<Frame> {
        if !self.valid || (is_nx && self.field_and_value.len() != 1) {
            return Ok(resp_invalid_arguments());
        }
        HashCommand::new(inner_db)
            .hset(&self.key, &self.field_and_value, is_hmset, is_nx)
            .await
    }
}

impl Default for Hset {
    fn default() -> Self {
        Hset {
            field_and_value: vec![],
            key: String::new(),
            valid: true,
        }
    }
}

impl Invalid for Hset {
    fn new_invalid() -> Hset {
        Hset {
            field_and_value: vec![],
            key: String::new(),
            valid: false,
        }
    }
}
