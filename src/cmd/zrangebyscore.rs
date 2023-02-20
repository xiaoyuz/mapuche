use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::{Buf, Bytes};
use slog::debug;

use crate::rocks::zset::ZsetCommand;
use crate::rocks::{get_client, Result as RocksResult};
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Zrangebyscore {
    key: String,
    min: f64,
    min_inclusive: bool,
    max: f64,
    max_inclusive: bool,
    withscores: bool,
    valid: bool,
}

impl Zrangebyscore {
    pub fn new(
        key: &str,
        min: f64,
        min_inclusive: bool,
        max: f64,
        max_inclusive: bool,
        withscores: bool,
    ) -> Zrangebyscore {
        Zrangebyscore {
            key: key.to_string(),
            min,
            min_inclusive,
            max,
            max_inclusive,
            withscores,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrangebyscore> {
        let key = parse.next_string()?;
        let mut min_inclusive = true;
        let mut max_inclusive = true;

        let mut min = 0f64;
        let mut max = 0f64;

        // parse score range as bytes, to handle exclusive bounder
        let mut bmin = parse.next_bytes()?;
        // check first byte
        if bmin[0] == b'(' {
            // drain the first byte
            bmin.advance(1);
            min_inclusive = false;
        } else if bmin == *"-inf" {
            min = f64::MIN;
        } else if bmin == *"+inf" {
            min = f64::MAX;
        }

        if min == 0f64 {
            min = String::from_utf8_lossy(&bmin).parse::<f64>().unwrap();
        }

        let mut bmax = parse.next_bytes()?;
        if bmax[0] == b'(' {
            bmax.advance(1);
            max_inclusive = false;
        } else if bmax == *"+inf" {
            max = f64::MAX;
        } else if bmax == *"-inf" {
            max = f64::MIN;
        }

        if max == 0f64 {
            max = String::from_utf8_lossy(&bmax).parse::<f64>().unwrap();
        }

        let mut withscores = false;
        // try to parse other flags
        while let Ok(v) = parse.next_string() {
            match v.to_uppercase().as_str() {
                // flags implement in signle command, such as ZRANGEBYSCORE
                "LIMIT" => {}
                "WITHSCORES" => {
                    withscores = true;
                }
                _ => {}
            }
        }

        let z = Zrangebyscore::new(&key, min, min_inclusive, max, max_inclusive, withscores);

        Ok(z)
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zrangebyscore> {
        if argv.len() < 3 {
            return Ok(Zrangebyscore::new_invalid());
        }
        let mut min_inclusive = true;
        let mut max_inclusive = true;
        let mut min = 0f64;
        let mut max: f64 = 0f64;

        // parse score range as bytes, to handle exclusive bounder
        let mut bmin = argv[1].clone();
        // check first byte
        if bmin[0] == b'(' {
            // drain the first byte
            bmin.advance(1);
            min_inclusive = false;
        } else if bmin == *"-inf" {
            min = f64::MIN;
        } else if bmin == *"+inf" {
            min = f64::MAX;
        }

        if min == 0f64 {
            min = String::from_utf8_lossy(&bmin).parse::<f64>().unwrap();
        }

        let mut bmax = argv[2].clone();
        if bmax[0] == b'(' {
            bmax.advance(1);
            max_inclusive = false;
        } else if bmax == *"+inf" {
            max = f64::MAX;
        } else if bmax == *"-inf" {
            max = f64::MIN;
        }

        if max == 0f64 {
            max = String::from_utf8_lossy(&bmax).parse::<f64>().unwrap();
        }

        let mut withscores = false;

        // try to parse other flags
        for v in &argv[2..] {
            match String::from_utf8_lossy(v).to_uppercase().as_str() {
                // flags implement in signle command, such as ZRANGEBYSCORE
                "LIMIT" => {}
                "WITHSCORES" => {
                    withscores = true;
                }
                _ => {}
            }
        }

        let z = Zrangebyscore::new(
            &String::from_utf8_lossy(&argv[0]),
            min,
            min_inclusive,
            max,
            max_inclusive,
            withscores,
        );

        Ok(z)
    }

    pub(crate) async fn apply(self, dst: &mut Connection, reverse: bool) -> crate::Result<()> {
        let response = self.zrangebyscore(reverse).await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zrangebyscore(&self, reverse: bool) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand::new(&get_client().await)
            .zrange_by_score(
                &self.key,
                self.min,
                self.min_inclusive,
                self.max,
                self.max_inclusive,
                self.withscores,
                reverse,
            )
            .await
    }
}

impl Invalid for Zrangebyscore {
    fn new_invalid() -> Zrangebyscore {
        Zrangebyscore {
            key: "".to_string(),
            min: 0f64,
            min_inclusive: false,
            max: 0f64,
            max_inclusive: false,
            withscores: false,
            valid: false,
        }
    }
}
