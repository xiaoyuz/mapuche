use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::parse::Parse;
use crate::rocks::kv::kvpair::KvPair;
use crate::rocks::string::StringCommand;
use crate::rocks::{get_client, KEY_ENCODER};
use crate::utils::resp_invalid_arguments;
use crate::{Connection, Frame, MapucheError};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use slog::debug;

use crate::rocks::Result as RocksResult;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Mset {
    keys: Vec<String>,
    vals: Vec<Bytes>,
    valid: bool,
}

impl Mset {
    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn vals(&self) -> &Vec<Bytes> {
        &self.vals
    }

    pub fn add_key(&mut self, key: String) {
        self.keys.push(key);
    }

    pub fn add_val(&mut self, val: Bytes) {
        self.vals.push(val);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Mset> {
        let mut mset = Mset::default();
        while let Ok(key) = parse.next_string() {
            mset.add_key(key);
            if let Ok(val) = parse.next_bytes() {
                mset.add_val(val);
            } else {
                return Err("protocol error".into());
            }
        }

        Ok(mset)
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Mset> {
        if argv.len() % 2 != 0 {
            return Ok(Mset::new_invalid());
        }
        let mut mset = Mset::default();
        for idx in (0..argv.len()).step_by(2) {
            mset.add_key(String::from_utf8_lossy(&argv[idx]).to_string());
            mset.add_val(argv[idx + 1].clone());
        }
        Ok(mset)
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.batch_put().await?;

        debug!(LOGGER, "res, {:?}", response);

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn batch_put(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        let mut kvs = Vec::new();
        for (idx, key) in self.keys.iter().enumerate() {
            let val = &self.vals[idx];
            let ekey = KEY_ENCODER.encode_string(key);
            let eval = KEY_ENCODER.encode_string_value(&mut val.to_vec(), -1);
            let kvpair = KvPair::from((ekey, eval));
            kvs.push(kvpair);
        }
        StringCommand::new(&get_client()).batch_put(kvs).await
    }

    pub fn hash_ring_key(&self) -> crate::Result<String> {
        if self.keys.len() != 1 {
            return Err(MapucheError::String("Cmd don't support cluster").into());
        }
        Ok((&self.keys.first().unwrap()).to_string())
    }
}

impl Default for Mset {
    /// Create a new `Mset` command which fetches `key` vector.
    fn default() -> Mset {
        Mset {
            keys: vec![],
            vals: vec![],
            valid: true,
        }
    }
}

impl Invalid for Mset {
    fn new_invalid() -> Mset {
        Mset {
            keys: vec![],
            vals: vec![],
            valid: false,
        }
    }
}
