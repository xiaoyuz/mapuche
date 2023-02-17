use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::{Buf, Bytes};
use slog::debug;

use crate::rocks::zset::ZsetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Zcount {
    key: String,
    min: f64,
    min_inclusive: bool,
    max: f64,
    max_inclusive: bool,
    valid: bool,
}

impl Zcount {
    pub fn new(key: &str, min: f64, min_inclusive: bool, max: f64, max_inclusive: bool) -> Zcount {
        Zcount {
            key: key.to_string(),
            min,
            min_inclusive,
            max,
            max_inclusive,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zcount> {
        let key = parse.next_string()?;
        let mut min_inclusive = true;
        let mut max_inclusive = true;

        // parse score range as bytes, to handle exclusive bounder
        let mut bmin = parse.next_bytes()?;
        // check first byte
        if bmin[0] == b'(' {
            // drain the first byte
            bmin.advance(1);
            min_inclusive = false;
        }
        let min = String::from_utf8_lossy(&bmin).parse::<f64>().unwrap();

        let mut bmax = parse.next_bytes()?;
        if bmax[0] == b'(' {
            bmax.advance(1);
            max_inclusive = false;
        }
        let max = String::from_utf8_lossy(&bmax).parse::<f64>().unwrap();

        let z = Zcount::new(&key, min, min_inclusive, max, max_inclusive);

        Ok(z)
    }

    #[allow(dead_code)]
    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zcount> {
        if argv.len() < 3 {
            return Ok(Zcount::new_invalid());
        }
        let mut min_inclusive = true;
        let mut max_inclusive = true;

        // parse score range as bytes, to handle exclusive bounder
        let mut bmin = argv[1].clone();
        // check first byte
        if bmin[0] == b'(' {
            // drain the first byte
            bmin.advance(1);
            min_inclusive = false;
        }
        let min = String::from_utf8_lossy(&bmin).parse::<f64>().unwrap();

        let mut bmax = argv[2].clone();
        if bmax[0] == b'(' {
            bmax.advance(1);
            max_inclusive = false;
        }
        let max = String::from_utf8_lossy(&bmax).parse::<f64>().unwrap();

        let z = Zcount::new(
            &String::from_utf8_lossy(&argv[0]),
            min,
            min_inclusive,
            max,
            max_inclusive,
        );
        Ok(z)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zcount().await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zcount(&self) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ZsetCommand
            .zcount(
                &self.key,
                self.min,
                self.min_inclusive,
                self.max,
                self.max_inclusive,
            )
            .await
    }
}

impl Invalid for Zcount {
    fn new_invalid() -> Zcount {
        Zcount {
            key: "".to_string(),
            min: 0f64,
            min_inclusive: false,
            max: 0f64,
            max_inclusive: false,
            valid: false,
        }
    }
}
