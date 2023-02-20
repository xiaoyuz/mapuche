use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;

use crate::rocks::zset::ZsetCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Zscore {
    key: String,
    member: String,
    valid: bool,
}

impl Zscore {
    pub fn new(key: &str, member: &str) -> Zscore {
        Zscore {
            key: key.to_string(),
            member: member.to_string(),
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zscore> {
        let key = parse.next_string()?;
        let member = parse.next_string()?;

        Ok(Zscore {
            key,
            member,
            valid: true,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zscore> {
        if argv.len() != 2 {
            return Ok(Zscore::new_invalid());
        }
        Ok(Zscore::new(
            &String::from_utf8_lossy(&argv[0]),
            &String::from_utf8_lossy(&argv[1]),
        ))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zscore().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zscore(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(&get_client().await)
            .zscore(&self.key, &self.member)
            .await
    }
}

impl Invalid for Zscore {
    fn new_invalid() -> Zscore {
        Zscore {
            key: "".to_string(),
            member: "".to_string(),
            valid: false,
        }
    }
}
