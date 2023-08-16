use crate::{Connection, Frame, Parse};

use crate::cmd::{retry_call, Invalid};
use bytes::Bytes;
use futures::FutureExt;
use serde::{Deserialize, Serialize};

use crate::rocks::set::SetCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Sadd {
    key: String,
    members: Vec<String>,
    valid: bool,
}

impl Sadd {
    pub fn new(key: &str) -> Sadd {
        Sadd {
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Sadd> {
        let key = parse.next_string()?;
        let mut sadd = Sadd::new(&key);
        while let Ok(member) = parse.next_string() {
            sadd.add_member(&member);
        }
        Ok(sadd)
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Sadd> {
        if argv.len() < 2 {
            return Ok(Sadd::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        let mut sadd = Sadd::new(key);
        for arg in &argv[1..] {
            sadd.add_member(&String::from_utf8_lossy(arg));
        }
        Ok(sadd)
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = retry_call(|| async move { self.sadd().await }.boxed()).await?;

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn sadd(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        SetCommand::new(&get_client())
            .sadd(&self.key, &self.members)
            .await
    }
}

impl Invalid for Sadd {
    fn new_invalid() -> Sadd {
        Sadd {
            key: "".to_string(),
            members: vec![],
            valid: false,
        }
    }
}
