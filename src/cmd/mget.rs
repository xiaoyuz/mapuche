use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::parse::Parse;
use crate::rocks::string::StringCommand;
use crate::utils::resp_invalid_arguments;
use crate::{Connection, Frame, MapucheError};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use slog::debug;

use crate::rocks::{get_client, Result as RocksResult};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Mget {
    /// Name of the keys to get
    keys: Vec<String>,
    valid: bool,
}

impl Mget {
    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn add_key(&mut self, key: String) {
        self.keys.push(key);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Mget> {
        // The `MGET` string has already been consumed. The next value is the
        // name of the key to get. If the next value is not a string or the
        // input is fully consumed, then an error is returned.
        let mut mget = Mget::default();

        while let Ok(key) = parse.next_string() {
            mget.add_key(key);
        }

        Ok(mget)
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Mget> {
        if argv.is_empty() {
            return Ok(Mget::new_invalid());
        }
        let mut mget = Mget::default();
        for arg in argv {
            mget.add_key(String::from_utf8_lossy(arg).to_string());
        }
        Ok(mget)
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.batch_get().await?;

        debug!(LOGGER, "res, {:?}", response);

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn batch_get(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand::new(&get_client())
            .batch_get(&self.keys)
            .await
    }

    pub fn hash_ring_key(&self) -> crate::Result<String> {
        if self.keys.len() != 1 {
            return Err(MapucheError::String("Cmd don't support cluster").into());
        }
        Ok((&self.keys.first().unwrap()).to_string())
    }
}

impl Default for Mget {
    /// Create a new `Mget` command which fetches `key` vector.
    fn default() -> Self {
        Mget {
            keys: vec![],
            valid: true,
        }
    }
}

impl Invalid for Mget {
    fn new_invalid() -> Mget {
        Mget {
            keys: vec![],
            valid: false,
        }
    }
}
