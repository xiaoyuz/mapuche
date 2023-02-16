use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Zcard {
    key: String,
    valid: bool,
}

impl Zcard {
    pub fn new(key: &str) -> Zcard {
        Zcard {
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zcard> {
        let key = parse.next_string()?;
        Ok(Zcard { key, valid: true })
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zcard> {
        if argv.len() != 1 {
            return Ok(Zcard::new_invalid());
        }
        Ok(Zcard::new(&String::from_utf8_lossy(&argv[0])))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zcard().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zcard(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand.zcard(&self.key).await
    }
}

impl Invalid for Zcard {
    fn new_invalid() -> Zcard {
        Zcard {
            key: "".to_owned(),
            valid: false,
        }
    }
}