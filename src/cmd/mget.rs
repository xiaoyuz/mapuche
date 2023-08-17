use crate::cmd::Invalid;
use crate::db::DBInner;

use crate::rocks::string::StringCommand;
use crate::utils::resp_invalid_arguments;
use crate::Frame;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Mget {
    /// Name of the keys to get
    keys: Vec<String>,
    valid: bool,
}

impl Mget {
    pub fn new(keys: &[impl ToString]) -> Mget {
        Mget {
            keys: keys.iter().map(|it| it.to_string()).collect(),
            valid: true,
        }
    }

    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand::new(inner_db).batch_get(&self.keys).await
    }
}

impl Default for Mget {
    /// Create a new `Mget` command which fetches `key` vector.
    fn default() -> Self {
        Mget {
            keys: vec![],
            valid: true,
        }
    }
}

impl Invalid for Mget {
    fn new_invalid() -> Mget {
        Mget {
            keys: vec![],
            valid: false,
        }
    }
}
