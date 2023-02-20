use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;

use crate::rocks::set::SetCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Spop {
    key: String,
    count: i64,
    valid: bool,
}

impl Spop {
    pub fn new(key: &str, count: i64) -> Spop {
        Spop {
            key: key.to_string(),
            count,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Spop> {
        let key = parse.next_string()?;

        let mut count = 1;
        if let Ok(v) = parse.next_int() {
            count = v;
        }
        Ok(Spop {
            key,
            count,
            valid: true,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Spop> {
        if argv.is_empty() || argv.len() > 2 {
            return Ok(Spop::new_invalid());
        }
        let mut count = 1;
        if argv.len() == 2 {
            match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
                Ok(v) => count = v,
                Err(_) => return Ok(Spop::new_invalid()),
            }
        }
        Ok(Spop::new(&String::from_utf8_lossy(&argv[0]), count))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.spop().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn spop(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        SetCommand::new(&get_client().await)
            .spop(&self.key, self.count as u64)
            .await
    }
}

impl Invalid for Spop {
    fn new_invalid() -> Spop {
        Spop {
            key: "".to_string(),
            count: 0,
            valid: false,
        }
    }
}
