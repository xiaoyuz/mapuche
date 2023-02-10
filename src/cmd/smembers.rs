use crate::{Connection, Frame, Parse};

use bytes::Bytes;
use tracing::{debug};
use crate::cmd::Invalid;

use crate::rocks::Result as RocksResult;
use crate::rocks::set::SetCommand;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Smembers {
    key: String,
    valid: bool,
}

impl Smembers {
    pub fn new(key: &str) -> Smembers {
        Smembers {
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Smembers> {
        let key = parse.next_string()?;
        Ok(Smembers::new(&key))
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Smembers> {
        if argv.len() != 1 {
            return Ok(Smembers {
                key: "".to_owned(),
                valid: false,
            });
        }
        Ok(Smembers::new(&String::from_utf8_lossy(&argv[0])))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.smembers().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn smembers(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        SetCommand
            .smembers(&self.key)
            .await
    }
}

impl Invalid for Smembers {
    fn new_invalid() -> Smembers {
        Smembers {
            key: "".to_owned(),
            valid: false,
        }
    }
}