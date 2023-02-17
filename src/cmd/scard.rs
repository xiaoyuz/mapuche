use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;

use crate::rocks::set::SetCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Scard {
    key: String,
    valid: bool,
}

impl Scard {
    pub fn new(key: &str) -> Scard {
        Scard {
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Scard> {
        let key = parse.next_string()?;
        Ok(Scard::new(&key))
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Scard> {
        if argv.len() != 1 {
            return Ok(Scard::new_invalid());
        }
        Ok(Scard::new(&String::from_utf8_lossy(&argv[0])))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.scard().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn scard(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        SetCommand::new(&get_client()).scard(&self.key).await
    }
}

impl Invalid for Scard {
    fn new_invalid() -> Scard {
        Scard {
            key: "".to_owned(),
            valid: false,
        }
    }
}
