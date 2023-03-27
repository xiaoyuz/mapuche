use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::rocks::list::ListCommand;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use slog::debug;

use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Lindex {
    key: String,
    idx: i64,
    valid: bool,
}

impl Lindex {
    pub fn new(key: &str, idx: i64) -> Lindex {
        Lindex {
            key: key.to_owned(),
            idx,
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Lindex> {
        let key = parse.next_string()?;
        let idx = parse.next_int()?;

        Ok(Lindex {
            key,
            idx,
            valid: true,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Lindex> {
        if argv.len() != 2 {
            return Ok(Lindex::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        let idx = match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Lindex::new_invalid()),
        };
        Ok(Lindex::new(key, idx))
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.lindex().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn lindex(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ListCommand::new(&get_client().await)
            .lindex(&self.key, self.idx)
            .await
    }
}

impl Invalid for Lindex {
    fn new_invalid() -> Lindex {
        Lindex {
            key: "".to_owned(),
            idx: 0,
            valid: false,
        }
    }
}
