use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use slog::debug;

use crate::rocks::zset::ZsetCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Zrank {
    key: String,
    member: String,
    valid: bool,
}

impl Zrank {
    pub fn new(key: &str, member: &str) -> Zrank {
        Zrank {
            key: key.to_string(),
            member: member.to_string(),
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrank> {
        let key = parse.next_string()?;
        let member = parse.next_string()?;

        Ok(Zrank {
            key,
            member,
            valid: true,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zrank> {
        if argv.len() != 2 {
            return Ok(Zrank::new_invalid());
        }
        Ok(Zrank::new(
            &String::from_utf8_lossy(&argv[0]),
            &String::from_utf8_lossy(&argv[1]),
        ))
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zrank().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zrank(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(&get_client().await)
            .zrank(&self.key, &self.member)
            .await
    }
}

impl Invalid for Zrank {
    fn new_invalid() -> Zrank {
        Zrank {
            key: "".to_string(),
            member: "".to_string(),
            valid: false,
        }
    }
}
