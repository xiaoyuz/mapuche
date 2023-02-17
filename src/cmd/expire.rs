use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::parse::Parse;
use crate::{Connection, Frame};
use bytes::Bytes;
use slog::debug;

use crate::rocks::string::StringCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::{resp_invalid_arguments, timestamp_from_ttl};

#[derive(Debug, Clone)]
pub struct Expire {
    key: String,
    seconds: i64,
    valid: bool,
}

impl Expire {
    pub fn new(key: impl ToString, seconds: i64) -> Expire {
        Expire {
            key: key.to_string(),
            seconds,
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn seconds(&self) -> i64 {
        self.seconds
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Expire> {
        let key = parse.next_string()?;
        let seconds = parse.next_int()?;

        Ok(Expire {
            key,
            seconds,
            valid: true,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Expire> {
        if argv.len() != 2 {
            return Ok(Expire::new_invalid());
        }
        let key = String::from_utf8_lossy(&argv[0]);
        match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
            Ok(v) => Ok(Expire::new(key, v)),
            Err(_) => Ok(Expire::new_invalid()),
        }
    }

    pub(crate) async fn apply(
        self,
        dst: &mut Connection,
        is_millis: bool,
        expire_at: bool,
    ) -> crate::Result<()> {
        let response = self
            .expire(is_millis, expire_at)
            .await
            .unwrap_or_else(Into::into);
        debug!(LOGGER, "res, {:?}", response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn expire(self, is_millis: bool, expire_at: bool) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        let mut ttl = self.seconds;
        if !is_millis {
            ttl *= 1000;
        }
        if !expire_at {
            ttl = timestamp_from_ttl(ttl);
        }
        StringCommand::new(get_client()).expire(&self.key, ttl).await
    }
}

impl Invalid for Expire {
    fn new_invalid() -> Expire {
        Expire {
            key: "".to_owned(),
            seconds: 0,
            valid: false,
        }
    }
}
