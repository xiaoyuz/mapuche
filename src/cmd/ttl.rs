use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use slog::debug;

use crate::rocks::string::StringCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TTL {
    key: String,
    valid: bool,
}

impl TTL {
    pub fn new(key: impl ToString) -> TTL {
        TTL {
            key: key.to_string(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<TTL> {
        let key = parse.next_string()?;

        Ok(TTL { key, valid: true })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<TTL> {
        if argv.len() != 1 {
            return Ok(TTL::new_invalid());
        }
        Ok(TTL {
            key: String::from_utf8_lossy(&argv[0]).to_string(),
            valid: true,
        })
    }

    pub(crate) async fn apply(&self, dst: &mut Connection, is_millis: bool) -> crate::Result<()> {
        let response = self.ttl(is_millis).await?;

        debug!(LOGGER, "res, {:?}", response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn ttl(&self, is_millis: bool) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand::new(&get_client())
            .ttl(&self.key, is_millis)
            .await
    }

    pub fn hash_ring_key(&self) -> crate::Result<String> {
        Ok(self.key.to_string())
    }
}

impl Invalid for TTL {
    fn new_invalid() -> TTL {
        TTL {
            key: "".to_owned(),
            valid: false,
        }
    }
}
