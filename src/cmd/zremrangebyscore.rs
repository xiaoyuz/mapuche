use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Zremrangebyscore {
    key: String,
    min: f64,
    max: f64,
    valid: bool,
}

impl Zremrangebyscore {
    pub fn new(key: &str, min: f64, max: f64) -> Zremrangebyscore {
        Zremrangebyscore {
            key: key.to_string(),
            min,
            max,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zremrangebyscore> {
        let key = parse.next_string()?;

        // TODO support (/-inf/+inf
        let min = parse.next_string()?.parse::<f64>()?;
        let max = parse.next_string()?.parse::<f64>()?;

        let z = Zremrangebyscore::new(&key, min, max);

        Ok(z)
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zremrangebyscore> {
        if argv.len() != 3 {
            return Ok(Zremrangebyscore::new_invalid());
        }
        // TODO
        let min = match String::from_utf8_lossy(&argv[1]).parse::<f64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zremrangebyscore::new_invalid()),
        };

        let max = match String::from_utf8_lossy(&argv[2]).parse::<f64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zremrangebyscore::new_invalid()),
        };

        Ok(Zremrangebyscore::new(
            &String::from_utf8_lossy(&argv[0]),
            min,
            max,
        ))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zremrangebyscore().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zremrangebyscore(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand
            .zremrange_by_score(&self.key, self.min, self.max)
            .await
    }
}

impl Invalid for Zremrangebyscore {
    fn new_invalid() -> Zremrangebyscore {
        Zremrangebyscore {
            key: "".to_string(),
            min: 0f64,
            max: 0f64,
            valid: false,
        }
    }
}
