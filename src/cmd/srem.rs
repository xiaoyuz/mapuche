use crate::{Connection, Frame, Parse};

use bytes::Bytes;
use tracing::{debug};
use crate::cmd::Invalid;

use crate::rocks::Result as RocksResult;
use crate::rocks::set::SetCommand;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Srem {
    key: String,
    members: Vec<String>,
    valid: bool,
}

impl Srem {
    pub fn new(key: &str) -> Srem {
        Srem {
            key: key.to_string(),
            members: vec![],
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn set_key(&mut self, key: &str) {
        self.key = key.to_owned();
    }

    pub fn add_member(&mut self, member: &str) {
        self.members.push(member.to_string());
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Srem> {
        let key = parse.next_string()?;
        let mut srem = Srem::new(&key);
        while let Ok(member) = parse.next_string() {
            srem.add_member(&member);
        }
        Ok(srem)
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Srem> {
        if argv.len() < 2 {
            return Ok(Srem::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        let mut srem = Srem::new(key);
        for arg in &argv[1..] {
            srem.add_member(&String::from_utf8_lossy(arg));
        }
        Ok(srem)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.srem().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn srem(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        SetCommand
            .srem(&self.key, &self.members)
            .await
    }
}

impl Invalid for Srem {
    fn new_invalid() -> Srem {
        Srem {
            key: "".to_string(),
            members: vec![],
            valid: false,
        }
    }
}