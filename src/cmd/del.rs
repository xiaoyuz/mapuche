use crate::{Connection, Frame, Parse};

use bytes::Bytes;
use tracing::{debug};
use crate::cmd::Invalid;

use crate::rocks::Result as RocksResult;
use crate::rocks::string::StringCommand;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Del {
    keys: Vec<String>,
    valid: bool,
}

impl Del {
    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn add_key(&mut self, key: String) {
        self.keys.push(key);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Del> {
        let mut del = Del::default();
        while let Ok(key) = parse.next_string() {
            del.add_key(key);
        }
        Ok(del)
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Del> {
        if argv.is_empty() {
            return Ok(Del {
                keys: vec![],
                valid: false,
            });
        }
        Ok(Del {
            keys: argv
                .iter()
                .map(|x| String::from_utf8_lossy(x).to_string())
                .collect::<Vec<String>>(),
            valid: true,
        })
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.del().await.unwrap_or_else(Into::into);

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn del(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand
            .del(&self.keys)
            .await
    }
}

impl Default for Del {
    /// Create a new `Del` command which fetches `key` vector.
    fn default() -> Self {
        Del {
            keys: vec![],
            valid: true,
        }
    }
}

impl Invalid for Del {
    fn new_invalid() -> Self {
        Del {
            keys: vec![],
            valid: false,
        }
    }
}
