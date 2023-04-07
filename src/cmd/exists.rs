use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::parse::Parse;
use crate::{Connection, Frame, MapucheError};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use slog::debug;

use crate::rocks::string::StringCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
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

    #[allow(dead_code)]
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

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.exists().await?;

        debug!(LOGGER, "res, {:?}", response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn exists(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand::new(&get_client().await)
            .exists(&self.keys)
            .await
    }

    pub fn hash_ring_key(&self) -> crate::Result<String> {
        if self.keys.len() != 1 {
            return Err(MapucheError::String("Cmd don't support cluster").into());
        }
        Ok((&self.keys.first().unwrap()).to_string())
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
