use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::rocks::hash::HashCommand;
use bytes::Bytes;
use slog::debug;

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Hget {
    key: String,
    field: String,
    valid: bool,
}

impl Hget {
    pub fn new(key: &str, field: &str) -> Hget {
        Hget {
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hget> {
        let key = parse.next_string()?;
        let field = parse.next_string()?;
        Ok(Hget::new(&key, &field))
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Hget> {
        if argv.len() != 2 {
            return Ok(Hget::new_invalid());
        }
        Ok(Hget::new(
            &String::from_utf8_lossy(&argv[0]),
            &String::from_utf8_lossy(&argv[1]),
        ))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.hget().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hget(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        HashCommand.hget(&self.key, &self.field).await
    }
}

impl Invalid for Hget {
    fn new_invalid() -> Hget {
        Hget {
            field: "".to_owned(),
            key: "".to_owned(),
            valid: false,
        }
    }
}
