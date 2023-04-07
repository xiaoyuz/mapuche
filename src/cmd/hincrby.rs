use crate::{Connection, Frame, Parse};

use crate::cmd::{retry_call, Invalid};
use crate::config::LOGGER;
use crate::rocks::hash::HashCommand;
use bytes::Bytes;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use slog::debug;

use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Hincrby {
    key: String,
    field: String,
    step: i64,
    valid: bool,
}

impl Hincrby {
    pub fn new(key: &str, field: &str, step: i64) -> Hincrby {
        Hincrby {
            key: key.to_string(),
            field: field.to_string(),
            step,
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn set_key(&mut self, key: &str) {
        self.key = key.to_owned();
    }

    pub fn set_field(&mut self, field: &str) {
        self.field = field.to_owned();
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hincrby> {
        let key = parse.next_string()?;
        let field = parse.next_string()?;
        let step = parse.next_int()?;
        Ok(Hincrby {
            key,
            field,
            step,
            valid: true,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Hincrby> {
        if argv.len() != 3 {
            return Ok(Hincrby::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        let field = &String::from_utf8_lossy(&argv[1]);
        let step = String::from_utf8_lossy(&argv[2]).parse::<i64>();
        match step {
            Ok(v) => Ok(Hincrby::new(key, field, v)),
            Err(_) => Ok(Hincrby::new_invalid()),
        }
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = retry_call(|| async move { self.hincrby().await }.boxed()).await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hincrby(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        HashCommand::new(&get_client().await)
            .hincrby(&self.key, &self.field, self.step)
            .await
    }

    pub fn hash_ring_key(&self) -> crate::Result<String> {
        Ok(self.key.to_string())
    }
}

impl Invalid for Hincrby {
    fn new_invalid() -> Hincrby {
        Hincrby {
            key: "".to_string(),
            field: "".to_string(),
            step: 0,
            valid: false,
        }
    }
}
