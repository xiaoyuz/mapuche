use crate::{Connection, Frame, Parse};

use crate::cmd::{retry_call, Invalid};
use crate::config::LOGGER;
use crate::rocks::hash::HashCommand;
use bytes::Bytes;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use slog::debug;

use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Hdel {
    key: String,
    fields: Vec<String>,
    valid: bool,
}

impl Hdel {
    pub fn new(key: &str) -> Hdel {
        Hdel {
            fields: vec![],
            key: key.to_owned(),
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn add_field(&mut self, field: &str) {
        self.fields.push(field.to_owned());
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hdel> {
        let key = parse.next_string()?;
        let mut hdel = Hdel::new(&key);
        while let Ok(f) = parse.next_string() {
            hdel.add_field(&f);
        }
        Ok(hdel)
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Hdel> {
        if argv.len() < 2 {
            return Ok(Hdel::new_invalid());
        }
        let mut hdel = Hdel::new(&String::from_utf8_lossy(&argv[0]));
        for arg in &argv[1..] {
            hdel.add_field(&String::from_utf8_lossy(arg));
        }
        Ok(hdel)
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = retry_call(|| async move { self.hdel().await }.boxed()).await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hdel(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        HashCommand::new(&get_client().await)
            .hdel(&self.key, &self.fields)
            .await
    }

    pub fn hash_ring_key(&self) -> crate::Result<String> {
        Ok(self.key.to_string())
    }
}

impl Invalid for Hdel {
    fn new_invalid() -> Hdel {
        Hdel {
            fields: vec![],
            key: "".to_owned(),
            valid: false,
        }
    }
}
