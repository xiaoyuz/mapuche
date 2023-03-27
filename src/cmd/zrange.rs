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
pub struct Zrange {
    key: String,
    min: i64,
    max: i64,
    withscores: bool,
    reverse: bool,
    valid: bool,
}

impl Zrange {
    pub fn new(key: &str, min: i64, max: i64, withscores: bool, reverse: bool) -> Zrange {
        Zrange {
            key: key.to_string(),
            min,
            max,
            withscores,
            reverse,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrange> {
        let key = parse.next_string()?;

        let min = parse.next_int()?;
        let max = parse.next_int()?;

        let mut withscores = false;
        let mut reverse = false;

        // try to parse other flags
        while let Ok(v) = parse.next_string() {
            match v.to_uppercase().as_str() {
                // flags implement in single command, such as ZRANGEBYSCORE
                "BYSCORE" => {}
                "BYLEX" => {}
                "REV" => {
                    reverse = true;
                }
                "LIMIT" => {}
                "WITHSCORES" => {
                    withscores = true;
                }
                _ => {}
            }
        }

        let z = Zrange::new(&key, min, max, withscores, reverse);

        Ok(z)
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zrange> {
        if argv.len() < 3 {
            return Ok(Zrange::new_invalid());
        }
        let min = match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zrange::new_invalid()),
        };
        let max = match String::from_utf8_lossy(&argv[2]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zrange::new_invalid()),
        };
        let mut withscores = false;
        let mut reverse = false;

        for arg in &argv[2..] {
            match String::from_utf8_lossy(arg).to_uppercase().as_str() {
                // flags implement in single command, such as ZRANGEBYSCORE
                "BYSCORE" => {}
                "BYLEX" => {}
                "REV" => {
                    reverse = true;
                }
                "LIMIT" => {}
                "WITHSCORES" => {
                    withscores = true;
                }
                _ => {}
            }
        }
        let z = Zrange::new(
            &String::from_utf8_lossy(&argv[0]),
            min,
            max,
            withscores,
            reverse,
        );

        Ok(z)
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zrange().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zrange(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(&get_client().await)
            .zrange(&self.key, self.min, self.max, self.withscores, self.reverse)
            .await
    }
}

impl Invalid for Zrange {
    fn new_invalid() -> Zrange {
        Zrange {
            key: "".to_string(),
            min: 0,
            max: 0,
            withscores: false,
            reverse: false,
            valid: false,
        }
    }
}
