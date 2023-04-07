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
pub struct Hstrlen {
    key: String,
    field: String,
    valid: bool,
}

impl Hstrlen {
    pub fn new(key: &str, field: &str) -> Hstrlen {
        Hstrlen {
            field: field.to_owned(),
            key: key.to_owned(),
            valid: true,
        }
    }

    pub fn new_invalid() -> Hstrlen {
        Hstrlen {
            field: "".to_owned(),
            key: "".to_owned(),
            valid: false,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hstrlen> {
        let key = parse.next_string()?;
        let field = parse.next_string()?;
        Ok(Hstrlen::new(&key, &field))
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Hstrlen> {
        if argv.len() != 2 {
            return Ok(Hstrlen::new_invalid());
        }
        Ok(Hstrlen::new(
            &String::from_utf8_lossy(&argv[0]),
            &String::from_utf8_lossy(&argv[1]),
        ))
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.hstrlen().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hstrlen(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        HashCommand::new(&get_client().await)
            .hstrlen(&self.key, &self.field)
            .await
    }

    pub fn hash_ring_key(&self) -> crate::Result<String> {
        Ok(self.key.to_string())
    }
}

impl Invalid for Hstrlen {
    fn new_invalid() -> Hstrlen {
        Hstrlen {
            field: "".to_owned(),
            key: "".to_owned(),
            valid: false,
        }
    }
}
