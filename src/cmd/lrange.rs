use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::rocks::list::ListCommand;
use bytes::Bytes;
use slog::debug;

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Lrange {
    key: String,
    left: i64,
    right: i64,
    valid: bool,
}

impl Lrange {
    pub fn new(key: &str, left: i64, right: i64) -> Lrange {
        Lrange {
            key: key.to_owned(),
            left,
            right,
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Lrange> {
        let key = parse.next_string()?;
        let left = parse.next_int()?;
        let right = parse.next_int()?;

        Ok(Lrange {
            key,
            left,
            right,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Lrange> {
        if argv.len() != 3 {
            return Ok(Lrange::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        let left = match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Lrange::new_invalid()),
        };

        let right = match String::from_utf8_lossy(&argv[2]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Lrange::new_invalid()),
        };
        Ok(Lrange::new(key, left, right))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.lrange().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn lrange(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ListCommand.lrange(&self.key, self.left, self.right).await
    }
}

impl Invalid for Lrange {
    fn new_invalid() -> Lrange {
        Lrange {
            key: "".to_owned(),
            left: 0,
            right: 0,
            valid: false,
        }
    }
}