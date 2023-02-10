use crate::{Connection, Frame, Parse};

use bytes::Bytes;
use tracing::{debug};
use crate::cmd::Invalid;

use crate::rocks::Result as RocksResult;
use crate::rocks::set::SetCommand;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Sismember {
    key: String,
    member: String,
    valid: bool,
}

impl Sismember {
    pub fn new(key: &str, member: &str) -> Sismember {
        Sismember {
            key: key.to_string(),
            member: member.to_string(),
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Sismember> {
        let key = parse.next_string()?;
        let member = parse.next_string()?;
        Ok(Sismember {
            key,
            member,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Sismember> {
        if argv.len() != 2 {
            return Ok(Sismember::new_invalid());
        }
        Ok(Sismember::new(
            &String::from_utf8_lossy(&argv[0]),
            &String::from_utf8_lossy(&argv[1]),
        ))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.sismember().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn sismember(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        let mut members = vec![];
        members.push(self.member.clone());
        SetCommand
            .sismember(&self.key, &members, false)
            .await
    }
}

impl Invalid for Sismember {
    fn new_invalid() -> Sismember {
        Sismember {
            key: "".to_string(),
            member: "".to_string(),
            valid: false,
        }
    }
}