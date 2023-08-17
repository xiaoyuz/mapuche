use crate::db::DBInner;
use crate::Frame;

use crate::cmd::Invalid;

use serde::{Deserialize, Serialize};

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Zadd {
    key: String,
    members: Vec<String>,
    scores: Vec<f64>,
    exists: Option<bool>,
    changed_only: bool,
    valid: bool,
}

impl Zadd {
    pub fn new(
        key: impl ToString,
        members: &[impl ToString],
        scores: &[f64],
        exists: Option<bool>,
        changed_only: bool,
    ) -> Zadd {
        Zadd {
            key: key.to_string(),
            members: members.iter().map(|it| it.to_string()).collect(),
            scores: scores.to_vec(),
            exists,
            changed_only,
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn execute(&mut self, inner_db: &DBInner) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(inner_db)
            .zadd(
                &self.key,
                &self.members,
                &self.scores,
                self.exists,
                self.changed_only,
                false,
            )
            .await
    }
}

impl Invalid for Zadd {
    fn new_invalid() -> Zadd {
        Zadd {
            key: "".to_string(),
            members: vec![],
            scores: vec![],
            exists: None,
            changed_only: false,
            valid: false,
        }
    }
}
