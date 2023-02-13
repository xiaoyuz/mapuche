use bytes::Bytes;
use slog::debug;
use crate::cmd::Invalid;
use crate::{Connection, Frame};
use crate::config::LOGGER;
use crate::parse::Parse;

use crate::rocks::Result as RocksResult;
use crate::rocks::string::StringCommand;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Exists {
    keys: Vec<String>,
    valid: bool,
}

impl Exists {
    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn add_key(&mut self, key: String) {
        self.keys.push(key)
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Exists> {
        let mut exists = Exists::default();

        while let Ok(key) = parse.next_string() {
            exists.add_key(key);
        }

        Ok(exists)
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Exists> {
        if argv.is_empty() {
            return Ok(Exists {
                keys: vec![],
                valid: false,
            });
        }
        Ok(Exists {
            keys: argv
                .iter()
                .map(|x| String::from_utf8_lossy(x).to_string())
                .collect::<Vec<String>>(),
            valid: true,
        })
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.exists().await.unwrap_or_else(Into::into);

        debug!(
            LOGGER,
            "res, {:?}",
            response
        );

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn exists(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand
            .exists(&self.keys)
            .await
    }
}

impl Default for Exists {
    fn default() -> Self {
        Exists {
            keys: vec![],
            valid: true,
        }
    }
}

impl Invalid for Exists {
    fn new_invalid() -> Exists {
        Exists {
            keys: vec![],
            valid: false,
        }
    }
}
