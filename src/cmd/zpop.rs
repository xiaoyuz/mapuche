use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;

use crate::rocks::zset::ZsetCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Zpop {
    key: String,
    count: i64,
    valid: bool,
}

impl Zpop {
    pub fn new(key: &str, count: i64) -> Zpop {
        Zpop {
            key: key.to_string(),
            count,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zpop> {
        let key = parse.next_string()?;
        // default count is 1
        let mut count = 1;
        if let Ok(c) = parse.next_int() {
            count = c;
        }
        Ok(Zpop {
            key,
            count,
            valid: true,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zpop> {
        if argv.is_empty() || argv.len() > 2 {
            return Ok(Zpop::new_invalid());
        }
        let mut count = 1;
        if argv.len() == 2 {
            match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
                Ok(v) => count = v,
                Err(_) => return Ok(Zpop::new_invalid()),
            }
        }
        Ok(Zpop::new(&String::from_utf8_lossy(&argv[0]), count))
    }

    pub(crate) async fn apply(self, dst: &mut Connection, from_min: bool) -> crate::Result<()> {
        let response = self.zpop(from_min).await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zpop(&self, from_min: bool) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(&get_client())
            .zpop(&self.key, from_min, self.count as u64)
            .await
    }
}

impl Invalid for Zpop {
    fn new_invalid() -> Zpop {
        Zpop {
            key: "".to_string(),
            count: 0,
            valid: false,
        }
    }
}
