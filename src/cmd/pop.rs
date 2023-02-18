use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::rocks::list::ListCommand;
use bytes::Bytes;
use slog::debug;

use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Pop {
    key: String,
    count: i64,
    valid: bool,
}

impl Pop {
    pub fn new(key: &str, count: i64) -> Pop {
        Pop {
            key: key.to_owned(),
            count,
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Pop> {
        if argv.is_empty() || argv.len() > 2 {
            return Ok(Pop::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        let mut count = 1;
        if argv.len() == 2 {
            match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
                Ok(v) => count = v,
                Err(_) => {
                    return Ok(Pop::new_invalid());
                }
            }
        }
        Ok(Pop::new(key, count))
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Pop> {
        let key = parse.next_string()?;
        let mut count = 1;

        if let Ok(n) = parse.next_int() {
            count = n;
        }

        let pop = Pop::new(&key, count);

        Ok(pop)
    }

    pub(crate) async fn apply(self, dst: &mut Connection, op_left: bool) -> crate::Result<()> {
        let response = self.pop(op_left).await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn pop(&self, op_left: bool) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ListCommand::new(&get_client())
            .pop(&self.key, op_left, self.count)
            .await
    }
}

impl Invalid for Pop {
    fn new_invalid() -> Pop {
        Pop {
            key: "".to_owned(),
            count: 0,
            valid: false,
        }
    }
}
