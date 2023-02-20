use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;

use crate::rocks::zset::ZsetCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Zrevrange {
    key: String,
    min: i64,
    max: i64,
    withscores: bool,
    valid: bool,
}

impl Zrevrange {
    pub fn new(key: &str, min: i64, max: i64, withscores: bool) -> Zrevrange {
        Zrevrange {
            key: key.to_string(),
            min,
            max,
            withscores,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrevrange> {
        let key = parse.next_string()?;

        let min = parse.next_int()?;
        let max = parse.next_int()?;

        let mut withscores = false;

        // try to parse other flags
        while let Ok(v) = parse.next_string() {
            if v.to_uppercase().as_str() == "WITHSCORES" {
                withscores = true;
            }
        }

        let z = Zrevrange::new(&key, min, max, withscores);

        Ok(z)
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zrevrange> {
        if argv.len() < 3 {
            return Ok(Zrevrange::new_invalid());
        }
        let min = match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zrevrange::new_invalid()),
        };
        let max = match String::from_utf8_lossy(&argv[2]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zrevrange::new_invalid()),
        };
        let mut withscores = false;

        for arg in &argv[2..] {
            // flags implement in single command, such as ZRANGEBYSCORE
            if String::from_utf8_lossy(arg).to_uppercase().as_str() == "WITHSCORES" {
                withscores = true;
            }
        }
        let z = Zrevrange::new(&String::from_utf8_lossy(&argv[0]), min, max, withscores);

        Ok(z)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zrevrange().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zrevrange(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(&get_client().await)
            .zrange(&self.key, self.min, self.max, self.withscores, true)
            .await
    }
}

impl Invalid for Zrevrange {
    fn new_invalid() -> Zrevrange {
        Zrevrange {
            key: "".to_string(),
            min: 0,
            max: 0,
            withscores: false,
            valid: false,
        }
    }
}
