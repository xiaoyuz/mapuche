use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::rocks::list::ListCommand;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Llen {
    key: String,
    valid: bool,
}

impl Llen {
    pub fn new(key: &str) -> Llen {
        Llen {
            key: key.to_owned(),
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Llen> {
        let key = parse.next_string()?;

        Ok(Llen { key, valid: true })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Llen> {
        if argv.len() != 1 {
            return Ok(Llen::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        Ok(Llen::new(key))
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.llen().await?;

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn llen(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ListCommand::new(&get_client()).llen(&self.key).await
    }
}

impl Invalid for Llen {
    fn new_invalid() -> Llen {
        Llen {
            key: "".to_owned(),
            valid: false,
        }
    }
}
