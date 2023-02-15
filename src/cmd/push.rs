use crate::{Connection, Frame, Parse};

use crate::cmd::Invalid;
use crate::config::LOGGER;
use crate::rocks::list::ListCommand;
use bytes::Bytes;
use slog::debug;

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Debug, Clone)]
pub struct Push {
    key: String,
    items: Vec<Bytes>,
    valid: bool,
}

impl Push {
    pub fn new(key: &str) -> Push {
        Push {
            items: vec![],
            key: key.to_owned(),
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn items(&self) -> &Vec<Bytes> {
        &self.items
    }

    pub fn add_item(&mut self, item: Bytes) {
        self.items.push(item);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Push> {
        let key = parse.next_string()?;
        let mut push = Push::new(&key);

        while let Ok(item) = parse.next_bytes() {
            push.add_item(item);
        }

        Ok(push)
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Push> {
        if argv.len() < 2 {
            return Ok(Push::new_invalid());
        }
        let mut push = Push::new(&String::from_utf8_lossy(&argv[0]));

        for arg in &argv[1..] {
            push.add_item(arg.to_owned());
        }

        Ok(push)
    }

    pub(crate) async fn apply(self, dst: &mut Connection, op_left: bool) -> crate::Result<()> {
        let response = self.push(op_left).await?;
        debug!(LOGGER, "res, {:?}", response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn push(&self, op_left: bool) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ListCommand.push(&self.key, &self.items, op_left).await
    }
}

impl Invalid for Push {
    fn new_invalid() -> Push {
        Push {
            items: vec![],
            key: "".to_owned(),
            valid: false,
        }
    }
}
