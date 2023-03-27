use crate::Command;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Message {
    PingMessage {
        address: String,
    },
    CmdMessage {
        address: String,
        cmd: Command,
        ts: i64,
    },
}

impl From<&Message> for String {
    fn from(value: &Message) -> Self {
        serde_json::to_string(value).unwrap()
    }
}

impl From<&str> for Message {
    fn from(value: &str) -> Self {
        serde_json::from_str(value).unwrap()
    }
}

impl From<&[u8]> for Message {
    fn from(value: &[u8]) -> Self {
        serde_json::from_slice(value).unwrap()
    }
}

impl From<Vec<u8>> for Message {
    fn from(value: Vec<u8>) -> Self {
        serde_json::from_slice(&value).unwrap()
    }
}

impl From<Message> for Vec<u8> {
    fn from(value: Message) -> Self {
        serde_json::to_vec(&value).unwrap()
    }
}
