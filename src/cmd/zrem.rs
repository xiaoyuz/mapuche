use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Zrem {
    key: String,
    members: Vec<String>,
    valid: bool,
}

impl Zrem {
    pub fn new(key: &str) -> Zrem {
        Zrem {
            key: key.to_string(),
            members: vec![],
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn set_key(&mut self, key: &str) {
        self.key = key.to_owned();
    }

    pub fn add_member(&mut self, member: &str) {
        self.members.push(member.to_string());
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrem> {
        let key = parse.next_string()?;
        let mut zrem = Zrem::new(&key);

        // parse member
        while let Ok(member) = parse.next_string() {
            zrem.add_member(&member);
        }

        Ok(zrem)
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zrem> {
        if argv.len() < 2 {
            return Ok(Zrem::new_invalid());
        }
        let mut zrem = Zrem::new(&String::from_utf8_lossy(&argv[0]));
        for arg in &argv[1..] {
            zrem.add_member(&String::from_utf8_lossy(arg));
        }
        Ok(zrem)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zrem().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zrem(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand.zrem(&self.key, &self.members).await
    }
}

impl Invalid for Zrem {
    fn new_invalid() -> Zrem {
        Zrem {
            key: "".to_string(),
            members: vec![],
            valid: false,
        }
    }
}
