use crate::{Connection, Frame, Parse};
use serde::{Deserialize, Serialize};

use crate::cmd::Invalid;
use crate::config::LOGGER;

use slog::debug;

use crate::rocks::string::StringCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Keys {
    regex: String,
    valid: bool,
}

impl Keys {
    pub fn new(regex: String) -> Keys {
        Keys { regex, valid: true }
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Keys> {
        let regex = parse.next_string()?;
        Ok(Keys { regex, valid: true })
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.keys().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    pub async fn keys(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand::new(&get_client()).keys(&self.regex).await
    }
}

impl Invalid for Keys {
    fn new_invalid() -> Keys {
        Keys {
            regex: "".to_owned(),
            valid: false,
        }
    }
}
