use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::rocks::list::ListCommand;
use bytes::Bytes;
use slog::debug;

use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Lset {
    key: String,
    idx: i64,
    element: Bytes,
    valid: bool,
}

impl Lset {
    pub fn new(key: &str, idx: i64, ele: Bytes) -> Lset {
        Lset {
            key: key.to_owned(),
            idx,
            element: ele,
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Lset> {
        let key = parse.next_string()?;
        let idx = parse.next_int()?;
        let element = parse.next_bytes()?;

        Ok(Lset {
            key,
            idx,
            element,
            valid: true,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Lset> {
        if argv.len() != 3 {
            return Ok(Lset::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        let idx = match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Lset::new_invalid()),
        };
        let ele = argv[2].clone();
        Ok(Lset::new(key, idx, ele))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.lset().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn lset(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ListCommand::new(&get_client())
            .lset(&self.key, self.idx, &self.element)
            .await
    }
}

impl Invalid for Lset {
    fn new_invalid() -> Lset {
        Lset {
            key: "".to_owned(),
            idx: 0,
            element: Bytes::new(),
            valid: false,
        }
    }
}
