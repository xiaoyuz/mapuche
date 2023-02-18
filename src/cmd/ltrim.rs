use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::rocks::list::ListCommand;
use bytes::Bytes;
use slog::debug;

use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Ltrim {
    key: String,
    start: i64,
    end: i64,
    valid: bool,
}

impl Ltrim {
    pub fn new(key: &str, start: i64, end: i64) -> Ltrim {
        Ltrim {
            key: key.to_owned(),
            start,
            end,
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ltrim> {
        let key = parse.next_string()?;
        let start = parse.next_int()?;
        let end = parse.next_int()?;

        Ok(Ltrim {
            key,
            start,
            end,
            valid: true,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Ltrim> {
        if argv.len() != 3 {
            return Ok(Ltrim::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        let start = match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Ltrim::new_invalid()),
        };

        let end = match String::from_utf8_lossy(&argv[2]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Ltrim::new_invalid()),
        };
        Ok(Ltrim::new(key, start, end))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.ltrim().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn ltrim(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ListCommand::new(&get_client())
            .ltrim(&self.key, self.start, self.end)
            .await
    }
}

impl Invalid for Ltrim {
    fn new_invalid() -> Ltrim {
        Ltrim {
            key: "".to_owned(),
            start: 0,
            end: 0,
            valid: false,
        }
    }
}
