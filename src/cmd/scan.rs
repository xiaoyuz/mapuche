use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::rocks::string::StringCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Scan {
    start: String,
    count: i64,
    regex: String,
    valid: bool,
}

impl Scan {
    pub fn new(start: String, count: i64, regex: String) -> Scan {
        Scan {
            start,
            count,
            regex,
            valid: true,
        }
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Scan> {
        let start = parse.next_string()?;
        let mut count = 10;
        let mut regex = ".*?".to_owned();
        while let Ok(flag) = parse.next_string() {
            if flag.to_uppercase().as_str() == "COUNT" {
                if let Ok(c) = parse.next_int() {
                    count = c;
                };
            } else if flag.to_uppercase().as_str() == "MATCH" {
                regex = parse.next_string()?;
            }
        }

        Ok(Scan {
            start,
            count,
            regex,
            valid: true,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Scan> {
        if argv.is_empty() || argv.len() > 5 {
            return Ok(Scan::new_invalid());
        }

        let mut count = 10;
        let mut regex = ".*?".to_owned();
        let start = String::from_utf8_lossy(&argv[0]);
        if argv.len() >= 3 {
            if argv[1].to_ascii_uppercase() == b"COUNT" {
                if let Ok(c) = String::from_utf8_lossy(&argv[2]).parse::<i64>() {
                    count = c;
                } else {
                    return Ok(Scan::new_invalid());
                }
            } else if argv[1].to_ascii_uppercase() == b"MATCH" {
                regex = String::from_utf8_lossy(&argv[2]).to_string();
            } else {
                return Ok(Scan::new_invalid());
            }
            if argv.len() == 5 {
                if argv[3].to_ascii_uppercase() == b"COUNT" {
                    if let Ok(c) = String::from_utf8_lossy(&argv[4]).parse::<i64>() {
                        count = c;
                    } else {
                        return Ok(Scan::new_invalid());
                    }
                } else if argv[3].to_ascii_uppercase() == b"MATCH" {
                    regex = String::from_utf8_lossy(&argv[4]).to_string();
                } else {
                    return Ok(Scan::new_invalid());
                }
            }
        }

        Ok(Scan {
            start: start.to_string(),
            count,
            regex,
            valid: true,
        })
    }

    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.scan().await?;
        dst.write_frame(&response).await?;
        Ok(())
    }

    pub async fn scan(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand::new(&get_client())
            .scan(&self.start, self.count.try_into().unwrap(), &self.regex)
            .await
    }
}

impl Invalid for Scan {
    fn new_invalid() -> Scan {
        Scan {
            start: "".to_owned(),
            count: 0,
            regex: "".to_owned(),
            valid: false,
        }
    }
}
