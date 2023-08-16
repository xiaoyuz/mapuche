use crate::cmd::Invalid;
use crate::parse::Parse;
use crate::{Connection, Frame};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::rocks::string::StringCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Type {
    key: String,
    valid: bool,
}

impl Type {
    pub fn new(key: impl ToString) -> Type {
        Type {
            key: key.to_string(),
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Type> {
        let key = parse.next_string()?;

        Ok(Type::new(key))
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Type> {
        if argv.len() != 1 {
            return Ok(Type::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        Ok(Type::new(key))
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.cmd_type().await?;

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn cmd_type(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand::new(&get_client()).get_type(&self.key).await
    }
}

impl Invalid for Type {
    fn new_invalid() -> Type {
        Type {
            key: "".to_owned(),
            valid: false,
        }
    }
}
