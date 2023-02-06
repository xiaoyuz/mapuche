use bytes::Bytes;
use tracing::debug;
use crate::cmd::Invalid;
use crate::{Connection, Frame};
use crate::parse::Parse;
use crate::rocks::errors::DECREMENT_OVERFLOW;

use crate::rocks::Result as RocksResult;
use crate::rocks::string::StringCommand;
use crate::utils::{resp_err, resp_invalid_arguments};

#[derive(Debug, Clone)]
pub struct IncrDecr {
    key: String,
    step: i64,
    valid: bool,
}

impl IncrDecr {
    pub fn new(key: impl ToString, step: i64) -> IncrDecr {
        IncrDecr {
            key: key.to_string(),
            step,
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse, single_step: bool) -> crate::Result<IncrDecr> {
        let key = parse.next_string()?;
        let step = if single_step { 1 } else { parse.next_int()? };
        Ok(IncrDecr {
            key,
            step,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>, single_step: bool) -> crate::Result<IncrDecr> {
        if (single_step && argv.len() != 1) || (!single_step && argv.len() != 2) {
            return Ok(IncrDecr::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        let step = if single_step {
            Ok(1)
        } else {
            String::from_utf8_lossy(&argv[1]).parse::<i64>()
        };

        match step {
            Ok(step) => Ok(IncrDecr::new(key, step)),
            Err(_) => Ok(IncrDecr::new_invalid()),
        }
    }

    pub(crate) async fn apply(mut self, dst: &mut Connection, inc: bool) -> crate::Result<()> {
        let response = self.incr_by(inc).await.unwrap_or_else(Into::into);

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn incr_by(&mut self, inc: bool) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }

        if !inc {
            if self.step == i64::MIN {
                return Ok(resp_err(DECREMENT_OVERFLOW));
            }
            self.step = -self.step;
        }

        StringCommand
            .raw_kv_incr(&self.key, self.step)
            .await
    }
}

impl Invalid for IncrDecr {
    fn new_invalid() -> IncrDecr {
        IncrDecr {
            key: "".to_owned(),
            step: 0,
            valid: false,
        }
    }
}