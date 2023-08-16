use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::rocks::set::SetCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Smismember {
    key: String,
    members: Vec<String>,
    valid: bool,
}

impl Smismember {
    pub fn new(key: &str) -> Smismember {
        Smismember {
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Smismember> {
        let key = parse.next_string()?;
        let mut smismember = Smismember::new(&key);
        while let Ok(member) = parse.next_string() {
            smismember.add_member(&member);
        }
        Ok(smismember)
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Smismember> {
        if argv.len() < 2 {
            return Ok(Smismember::new_invalid());
        }
        let mut s = Smismember::new(&String::from_utf8_lossy(&argv[0]));
        for arg in &argv[1..] {
            s.add_member(&String::from_utf8_lossy(arg));
        }
        Ok(s)
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.smismember().await?;

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn smismember(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        SetCommand::new(&get_client())
            .sismember(&self.key, &self.members, true)
            .await
    }
}

impl Invalid for Smismember {
    fn new_invalid() -> Smismember {
        Smismember {
            key: "".to_string(),
            members: vec![],
            valid: false,
        }
    }
}
