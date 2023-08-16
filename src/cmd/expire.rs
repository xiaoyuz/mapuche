use crate::cmd::{retry_call, Invalid};
use crate::db::DBInner;

use crate::Frame;

use futures::FutureExt;
use serde::{Deserialize, Serialize};

use crate::rocks::string::StringCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::{resp_invalid_arguments, timestamp_from_ttl};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Expire {
    key: String,
    seconds: i64,
    valid: bool,
}

impl Expire {
    pub fn new(key: impl ToString, seconds: i64) -> Expire {
        Expire {
            key: key.to_string(),
            seconds,
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn seconds(&self) -> i64 {
        self.seconds
    }

    pub async fn execute(
        &self,
        inner_db: &DBInner,
        is_millis: bool,
        expire_at: bool,
    ) -> RocksResult<Frame> {
        let response = retry_call(|| {
            async move {
                if !self.valid {
                    return Ok(resp_invalid_arguments());
                }
                let mut ttl = self.seconds;
                if !is_millis {
                    ttl *= 1000;
                }
                if !expire_at {
                    ttl = timestamp_from_ttl(ttl);
                }
                StringCommand::new(inner_db)
                    .expire(&self.key, ttl)
                    .await
                    .map_err(Into::into)
            }
            .boxed()
        })
        .await?;
        Ok(response)
    }
}

impl Invalid for Expire {
    fn new_invalid() -> Expire {
        Expire {
            key: "".to_owned(),
            seconds: 0,
            valid: false,
        }
    }
}
