use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;

use crate::rocks::zset::ZsetCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Zremrangebyrank {
    key: String,
    min: i64,
    max: i64,
    valid: bool,
}

impl Zremrangebyrank {
    pub fn new(key: &str, min: i64, max: i64) -> Zremrangebyrank {
        Zremrangebyrank {
            key: key.to_string(),
            min,
            max,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zremrangebyrank> {
        let key = parse.next_string()?;

        let min = parse.next_int()?;
        let max = parse.next_int()?;

        let z = Zremrangebyrank::new(&key, min, max);

        Ok(z)
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zremrangebyrank> {
        if argv.len() != 3 {
            return Ok(Zremrangebyrank::new_invalid());
        }
        let min = match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zremrangebyrank::new_invalid()),
        };

        let max = match String::from_utf8_lossy(&argv[2]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zremrangebyrank::new_invalid()),
        };

        Ok(Zremrangebyrank::new(
            &String::from_utf8_lossy(&argv[0]),
            min,
            max,
        ))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zremrangebyrank().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zremrangebyrank(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(&get_client())
            .zremrange_by_rank(&self.key, self.min, self.max)
            .await
    }
}

impl Invalid for Zremrangebyrank {
    fn new_invalid() -> Zremrangebyrank {
        Zremrangebyrank {
            key: "".to_string(),
            min: 0,
            max: 0,
            valid: false,
        }
    }
}
