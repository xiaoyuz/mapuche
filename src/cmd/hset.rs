use crate::{Connection, Frame, Parse};

use crate::cmd::{retry_call, Invalid};
use crate::config::LOGGER;
use crate::rocks::hash::HashCommand;
use crate::rocks::kv::kvpair::KvPair;
use bytes::Bytes;
use futures::FutureExt;
use slog::debug;

use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Hset {
    key: String,
    field_and_value: Vec<KvPair>,
    valid: bool,
}

impl Hset {
    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn set_key(&mut self, key: &str) {
        self.key = key.to_owned();
    }

    /// Get the field and value pairs
    pub fn fields(&self) -> &Vec<KvPair> {
        &self.field_and_value
    }

    pub fn add_field_value(&mut self, kv: KvPair) {
        self.field_and_value.push(kv);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hset> {
        let mut hset = Hset::default();

        let key = parse.next_string()?;
        hset.set_key(&key);

        while let Ok(field) = parse.next_string() {
            if let Ok(value) = parse.next_bytes() {
                let kv = KvPair::new(field, value.to_vec());
                hset.add_field_value(kv);
            } else {
                return Err("protocol error".into());
            }
        }
        Ok(hset)
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Hset> {
        if argv.len() % 2 != 1 {
            return Ok(Hset::new_invalid());
        }
        let key = String::from_utf8_lossy(&argv[0]).to_string();
        let mut hset = Hset::default();
        hset.set_key(&key);

        for idx in (1..argv.len()).step_by(2) {
            let field = String::from_utf8_lossy(&argv[idx]);
            let value = argv[idx + 1].clone();
            let kv = KvPair::new(field.to_string(), value);
            hset.add_field_value(kv);
        }
        Ok(hset)
    }

    pub(crate) async fn apply(
        &self,
        dst: &mut Connection,
        is_hmset: bool,
        is_nx: bool,
    ) -> crate::Result<()> {
        let response =
            retry_call(|| async move { self.hset(is_hmset, is_nx).await }.boxed()).await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hset(&self, is_hmset: bool, is_nx: bool) -> RocksResult<Frame> {
        if !self.valid || (is_nx && self.field_and_value.len() != 1) {
            return Ok(resp_invalid_arguments());
        }

        HashCommand::new(&get_client().await)
            .hset(&self.key, &self.field_and_value, is_hmset, is_nx)
            .await
    }
}

impl Default for Hset {
    fn default() -> Self {
        Hset {
            field_and_value: vec![],
            key: String::new(),
            valid: true,
        }
    }
}

impl Invalid for Hset {
    fn new_invalid() -> Hset {
        Hset {
            field_and_value: vec![],
            key: String::new(),
            valid: false,
        }
    }
}
