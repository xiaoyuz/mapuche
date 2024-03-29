use crate::{Connection, Frame, Parse};

use crate::cmd::{retry_call, Invalid};
use crate::config::LOGGER;
use bytes::Bytes;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use slog::debug;

use crate::rocks::zset::ZsetCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Zadd {
    key: String,
    members: Vec<String>,
    scores: Vec<f64>,
    exists: Option<bool>,
    changed_only: bool,
    valid: bool,
}

impl Zadd {
    pub fn new(key: &str) -> Zadd {
        Zadd {
            key: key.to_string(),
            members: vec![],
            scores: vec![],
            exists: None,
            changed_only: false,
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

    pub fn set_exists(&mut self, exists: bool) {
        self.exists = Some(exists);
    }

    pub fn set_changed_only(&mut self, changed_only: bool) {
        self.changed_only = changed_only;
    }

    pub fn add_member(&mut self, member: &str) {
        self.members.push(member.to_string());
    }

    pub fn add_score(&mut self, score: f64) {
        self.scores.push(score);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zadd> {
        let key = parse.next_string()?;
        let mut zadd = Zadd::new(&key);
        let mut first_score: Option<f64>;

        // try to parse the flag
        loop {
            match parse.next_string() {
                Ok(s) if s.to_uppercase() == "NX" => {
                    zadd.set_exists(false);
                }
                Ok(s) if s.to_uppercase() == "XX" => {
                    zadd.set_exists(true);
                }
                Ok(s) if s.to_uppercase() == "CH" => zadd.set_changed_only(true),
                Ok(s) if s.to_uppercase() == "GT" => {
                    // TODO:
                }
                Ok(s) if s.to_uppercase() == "LT" => {
                    // TODO:
                }
                Ok(s) if s.to_uppercase() == "INCR" => {
                    // TODO:
                }
                Ok(s) => {
                    // check if this is a score args
                    match String::from_utf8_lossy(s.as_bytes()).parse::<f64>() {
                        Ok(score) => {
                            first_score = Some(score);
                            // flags parse done
                            break;
                        }
                        Err(err) => {
                            // not support flags
                            return Err(err.into());
                        }
                    }
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        }

        // parse the score and member
        loop {
            if let Some(score) = first_score {
                // consume the score in last parse
                zadd.add_score(score);
                // reset first_score to None
                first_score = None;

                // parse next member
                let member = parse.next_string()?;
                zadd.add_member(&member);
            } else if let Ok(str_score) = parse.next_string() {
                let member = parse.next_string()?;
                let score = String::from_utf8_lossy(str_score.as_bytes()).parse::<f64>()?;
                zadd.add_score(score);
                zadd.add_member(&member);
            } else {
                break;
            }
        }

        Ok(zadd)
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zadd> {
        if argv.is_empty() {
            return Ok(Zadd::new_invalid());
        }
        let mut zadd = Zadd::new(&String::from_utf8_lossy(&argv[0]));
        let mut first_score: Option<f64>;

        // try to parse the flag
        let mut idx = 1;
        loop {
            let arg = String::from_utf8_lossy(&argv[idx]).to_uppercase();
            if idx >= argv.len() {
                return Ok(Zadd::new_invalid());
            }
            match arg.as_str() {
                "NX" => {
                    zadd.set_exists(false);
                }
                "XX" => {
                    zadd.set_exists(true);
                }
                "CH" => zadd.set_changed_only(true),
                "GT" => {
                    // TODO:
                }
                "LT" => {
                    // TODO:
                }
                "INCR" => {
                    // TODO:
                }
                _ => {
                    // check if this is a score args
                    match String::from_utf8_lossy(arg.as_bytes()).parse::<f64>() {
                        Ok(score) => {
                            first_score = Some(score);
                            // flags parse done
                            break;
                        }
                        Err(_) => {
                            // not support flags
                            return Ok(Zadd::new_invalid());
                        }
                    }
                }
            }
            idx += 1;
        }

        // parse the score and member
        loop {
            if let Some(score) = first_score {
                // consume the score in last parse
                zadd.add_score(score);
                // reset first_score to None
                first_score = None;

                // parse next member
                idx += 1;
                if idx >= argv.len() {
                    return Ok(Zadd::new_invalid());
                }
                let member = &String::from_utf8_lossy(&argv[idx]);
                zadd.add_member(member);
            } else {
                idx += 1;
                if idx == argv.len() {
                    break;
                }
                if let Ok(score) = String::from_utf8_lossy(&argv[idx]).parse::<f64>() {
                    idx += 1;
                    if idx >= argv.len() {
                        return Ok(Zadd::new_invalid());
                    }
                    let member = &String::from_utf8_lossy(&argv[idx]);
                    zadd.add_score(score);
                    zadd.add_member(member);
                } else {
                    return Ok(Zadd::new_invalid());
                }
            }
        }

        Ok(zadd)
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = retry_call(|| async move { self.zadd().await }.boxed()).await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zadd(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(&get_client())
            .zadd(
                &self.key,
                &self.members,
                &self.scores,
                self.exists,
                self.changed_only,
                false,
            )
            .await
    }

    pub fn hash_ring_key(&self) -> crate::Result<String> {
        Ok(self.key.to_string())
    }
}

impl Invalid for Zadd {
    fn new_invalid() -> Zadd {
        Zadd {
            key: "".to_string(),
            members: vec![],
            scores: vec![],
            exists: None,
            changed_only: false,
            valid: false,
        }
    }
}
