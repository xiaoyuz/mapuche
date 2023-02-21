use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;

use crate::rocks::zset::ZsetCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Zincrby {
    key: String,
    step: f64,
    member: String,
    valid: bool,
}

impl Zincrby {
    pub fn new(key: &str, step: f64, member: &str) -> Zincrby {
        Zincrby {
            key: key.to_string(),
            step,
            member: member.to_string(),
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zincrby> {
        let key = parse.next_string()?;
        let step_byte = parse.next_bytes()?;
        let member = parse.next_string()?;

        let step = String::from_utf8_lossy(&step_byte).parse::<f64>()?;

        Ok(Zincrby::new(&key, step, &member))
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zincrby> {
        if argv.len() != 3 {
            return Ok(Zincrby::new_invalid());
        }

        let key = &String::from_utf8_lossy(&argv[0]);
        let step = String::from_utf8_lossy(&argv[1]).parse::<f64>()?;
        let member = &String::from_utf8_lossy(&argv[2]);

        Ok(Zincrby::new(key, step, member))
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zincrby().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zincrby(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }

        ZsetCommand::new(&get_client().await)
            .zincrby(&self.key, self.step, &self.member)
            .await
    }
}

impl Invalid for Zincrby {
    fn new_invalid() -> Zincrby {
        Zincrby {
            key: "".to_string(),
            member: "".to_string(),
            step: 0f64,
            valid: false,
        }
    }
}
