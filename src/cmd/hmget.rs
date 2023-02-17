use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::rocks::hash::HashCommand;
use bytes::Bytes;
use slog::debug;

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Hmget {
    key: String,
    fields: Vec<String>,
    valid: bool,
}

impl Hmget {
    pub fn new(key: &str) -> Hmget {
        Hmget {
            key: key.to_owned(),
            fields: vec![],
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn fields(&self) -> &Vec<String> {
        &self.fields
    }

    pub fn add_field(&mut self, field: &str) {
        self.fields.push(field.to_string());
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hmget> {
        let key = parse.next_string()?;
        let mut hmget = Hmget::new(&key);
        while let Ok(field) = parse.next_string() {
            hmget.add_field(&field);
        }
        Ok(hmget)
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Hmget> {
        if argv.len() < 2 {
            return Ok(Hmget::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        let mut hmget = Hmget::new(key);
        for arg in &argv[1..argv.len()] {
            hmget.add_field(&String::from_utf8_lossy(arg));
        }
        Ok(hmget)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.hmget().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hmget(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        HashCommand.hmget(&self.key, &self.fields).await
    }
}

impl Invalid for Hmget {
    fn new_invalid() -> Hmget {
        Hmget {
            key: "".to_owned(),
            fields: vec![],
            valid: false,
        }
    }
}
