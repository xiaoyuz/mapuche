use bytes::Bytes;
use tracing::debug;
use crate::cmd::Invalid;
use crate::{Connection, Frame};
use crate::parse::Parse;
use crate::rocks::string::StringCommand;
use crate::utils::resp_invalid_arguments;

use crate::rocks::Result as RocksResult;

#[derive(Debug, Clone)]
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

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.batch_get().await.unwrap_or_else(Into::into);

        debug!(?response);

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn batch_get(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand
            .raw_kv_batch_get(&self.keys)
            .await
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