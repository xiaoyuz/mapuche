use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::rocks::hash::HashCommand;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use slog::debug;

use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Hvals {
    key: String,
    valid: bool,
}

impl Hvals {
    pub fn new(key: &str) -> Hvals {
        Hvals {
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hvals> {
        let key = parse.next_string()?;
        Ok(Hvals { key, valid: true })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Hvals> {
        if argv.len() != 1 {
            return Ok(Hvals::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        Ok(Hvals::new(key))
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.hvals().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hvals(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        HashCommand::new(&get_client().await)
            .hgetall(&self.key, false, true)
            .await
    }

    pub fn hash_ring_key(&self) -> crate::Result<String> {
        Ok(self.key.to_string())
    }
}

impl Invalid for Hvals {
    fn new_invalid() -> Hvals {
        Hvals {
            key: "".to_owned(),
            valid: false,
        }
    }
}
