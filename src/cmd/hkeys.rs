use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::rocks::hash::HashCommand;
use bytes::Bytes;
use slog::debug;

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Hkeys {
    key: String,
    valid: bool,
}

impl Hkeys {
    pub fn new(key: &str) -> Hkeys {
        Hkeys {
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hkeys> {
        let key = parse.next_string()?;
        Ok(Hkeys { key, valid: true })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Hkeys> {
        if argv.len() != 1 {
            return Ok(Hkeys::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        Ok(Hkeys::new(key))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.hkeys().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hkeys(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        HashCommand.hgetall(&self.key, true, false).await
    }
}

impl Invalid for Hkeys {
    fn new_invalid() -> Hkeys {
        Hkeys {
            key: "".to_owned(),
            valid: false,
        }
    }
}
