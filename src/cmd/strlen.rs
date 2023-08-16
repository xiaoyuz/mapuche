use crate::cmd::Invalid;
use crate::parse::Parse;
use crate::{Connection, Frame};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::rocks::string::StringCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Strlen {
    key: String,
    valid: bool,
}

impl Strlen {
    pub fn new(key: impl ToString) -> Strlen {
        Strlen {
            key: key.to_string(),
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Strlen> {
        let key = parse.next_string()?;

        Ok(Strlen { key, valid: true })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Strlen> {
        if argv.len() != 1 {
            return Ok(Strlen::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        Ok(Strlen::new(key))
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.strlen().await?;

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn strlen(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand::new(&get_client()).strlen(&self.key).await
    }
}

impl Invalid for Strlen {
    fn new_invalid() -> Strlen {
        Strlen {
            key: "".to_owned(),
            valid: false,
        }
    }
}
