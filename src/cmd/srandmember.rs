use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;

use crate::rocks::set::SetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Srandmember {
    key: String,
    count: Option<i64>,
    valid: bool,
}

impl Srandmember {
    pub fn new(key: &str, count: Option<i64>) -> Srandmember {
        Srandmember {
            key: key.to_string(),
            count,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Srandmember> {
        let key = parse.next_string()?;

        let mut count = None;
        if let Ok(v) = parse.next_int() {
            count = Some(v);
        }
        Ok(Srandmember {
            key,
            count,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Srandmember> {
        if argv.is_empty() || argv.len() > 2 {
            return Ok(Srandmember::new_invalid());
        }
        let mut count = None;
        if argv.len() == 2 {
            match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
                Ok(v) => count = Some(v),
                Err(_) => return Ok(Srandmember::new_invalid()),
            }
        }
        Ok(Srandmember::new(&String::from_utf8_lossy(&argv[0]), count))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.srandmember().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn srandmember(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        let mut count;
        let repeatable;
        let array_resp;
        if self.count.is_none() {
            repeatable = false;
            count = 1;
            array_resp = false;
        } else {
            array_resp = true;
            count = self.count.unwrap();
            if count > 0 {
                repeatable = false;
            } else {
                repeatable = true;
                count = -count;
            }
        }
        SetCommand
            .srandmemeber(&self.key, count, repeatable, array_resp)
            .await
    }
}

impl Invalid for Srandmember {
    fn new_invalid() -> Srandmember {
        Srandmember {
            key: "".to_string(),
            count: None,
            valid: false,
        }
    }
}
