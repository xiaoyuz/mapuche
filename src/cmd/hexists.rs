use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::rocks::hash::HashCommand;
use bytes::Bytes;
use slog::debug;

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Hexists {
    key: String,
    field: String,
    valid: bool,
}

impl Hexists {
    pub fn new(key: &str, field: &str) -> Hexists {
        Hexists {
            field: field.to_owned(),
            key: key.to_owned(),
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hexists> {
        let key = parse.next_string()?;
        let field = parse.next_string()?;
        Ok(Hexists::new(&key, &field))
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Hexists> {
        if argv.len() != 2 {
            return Ok(Hexists::new_invalid());
        }
        Ok(Hexists::new(
            &String::from_utf8_lossy(&argv[0]),
            &String::from_utf8_lossy(&argv[1]),
        ))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.hexists().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hexists(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        HashCommand.hexists(&self.key, &self.field).await
    }
}

impl Invalid for Hexists {
    fn new_invalid() -> Hexists {
        Hexists {
            field: "".to_owned(),
            key: "".to_owned(),
            valid: false,
        }
    }
}
