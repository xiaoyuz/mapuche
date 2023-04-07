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
pub struct Lrem {
    key: String,
    count: i64,
    element: Bytes,
    valid: bool,
}

impl Lrem {
    pub fn new(key: &str, count: i64, element: Bytes) -> Lrem {
        Lrem {
            key: key.to_owned(),
            count,
            element,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Lrem> {
        let key = parse.next_string()?;
        let count = parse.next_int()?;
        let element = parse.next_bytes()?;

        Ok(Lrem {
            key,
            count,
            element,
            valid: true,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Lrem> {
        if argv.len() != 3 {
            return Ok(Lrem::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        let count = String::from_utf8_lossy(&argv[1]).parse::<i64>()?;

        let element = argv[2].clone();
        Ok(Lrem::new(key, count, element))
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = retry_call(|| async move { self.lrem().await }.boxed()).await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn lrem(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        let mut from_head = true;
        let mut count = self.count;
        if self.count < 0 {
            from_head = false;
            count = -count;
        }
        ListCommand::new(&get_client().await)
            .lrem(&self.key, count as usize, from_head, &self.element)
            .await
    }

    pub fn hash_ring_key(&self) -> crate::Result<String> {
        Ok(self.key.to_string())
    }
}

impl Invalid for Lrem {
    fn new_invalid() -> Lrem {
        Lrem {
            key: "".to_owned(),
            count: 0,
            element: Bytes::new(),
            valid: false,
        }
    }
}
