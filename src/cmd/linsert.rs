use crate::{Connection, Frame, Parse};

use crate::cmd::{retry_call, Invalid};
use crate::config::LOGGER;
use crate::rocks::list::ListCommand;
use bytes::Bytes;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use slog::debug;

use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Linsert {
    key: String,
    before_pivot: bool,
    pivot: Bytes,
    element: Bytes,
    valid: bool,
}

impl Linsert {
    pub fn new(key: &str, before_pivot: bool, pivot: Bytes, element: Bytes) -> Linsert {
        Linsert {
            key: key.to_owned(),
            before_pivot,
            pivot,
            element,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Linsert> {
        let key = parse.next_string()?;
        let pos = parse.next_string()?;
        let before_pivot = match pos.to_lowercase().as_str() {
            "before" => true,
            "after" => false,
            _ => {
                return Ok(Linsert::new_invalid());
            }
        };
        let pivot = parse.next_bytes()?;
        let element = parse.next_bytes()?;

        Ok(Linsert {
            key,
            before_pivot,
            pivot,
            element,
            valid: true,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Linsert> {
        if argv.len() != 4 {
            return Ok(Linsert::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        let before_pivot = match String::from_utf8_lossy(&argv[1]).to_lowercase().as_str() {
            "before" => true,
            "after" => false,
            _ => {
                return Ok(Linsert::new_invalid());
            }
        };

        let pivot = argv[2].clone();
        let element = argv[3].clone();
        Ok(Linsert::new(key, before_pivot, pivot, element))
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = retry_call(|| async move { self.linsert().await }.boxed()).await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn linsert(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ListCommand::new(&get_client().await)
            .linsert(&self.key, self.before_pivot, &self.pivot, &self.element)
            .await
    }

    pub fn hash_ring_key(&self) -> crate::Result<String> {
        Ok(self.key.to_string())
    }
}

impl Invalid for Linsert {
    fn new_invalid() -> Linsert {
        Linsert {
            key: "".to_owned(),
            before_pivot: false,
            pivot: Bytes::new(),
            element: Bytes::new(),
            valid: false,
        }
    }
}
