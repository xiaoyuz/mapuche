use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::rocks::hash::HashCommand;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Hgetall {
    key: String,
    valid: bool,
}

impl Hgetall {
    pub fn new(key: &str) -> Hgetall {
        Hgetall {
            key: key.to_string(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn set_key(&mut self, key: &str) {
        self.key = key.to_owned();
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hgetall> {
        let key = parse.next_string()?;
        Ok(Hgetall { key, valid: true })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Hgetall> {
        if argv.len() != 1 {
            return Ok(Hgetall::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        Ok(Hgetall::new(key))
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.hgetall().await?;

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hgetall(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        HashCommand::new(&get_client())
            .hgetall(&self.key, true, true)
            .await
    }
}

impl Invalid for Hgetall {
    fn new_invalid() -> Hgetall {
        Hgetall {
            key: "".to_string(),
            valid: false,
        }
    }
}
