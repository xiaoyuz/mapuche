pub mod decode;
pub mod encode;

#[derive(Debug, Clone)]
pub enum DataType {
    String,
    Hash,
    List,
    Set,
    Zset,
    Null,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataType::String => write!(f, "string"),
            DataType::Hash => write!(f, "hash"),
            DataType::List => write!(f, "list"),
            DataType::Set => write!(f, "set"),
            DataType::Zset => write!(f, "zset"),
            DataType::Null => write!(f, "none"),
        }
    }
}

use std::fmt;
pub use {decode::KeyDecoder, encode::KeyEncoder};

const SIGN_MASK: u64 = 0x8000000000000000;

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = b'\xff';
const ENC_ASC_PADDING: [u8; ENC_GROUP_SIZE] = [0; ENC_GROUP_SIZE];

fn encode_bytes(key: &[u8]) -> Vec<u8> {
    let len = key.len();
    let mut index = 0;
    let mut enc = vec![];
    while index <= len {
        let remain = len - index;
        let mut pad: usize = 0;
        if remain > ENC_GROUP_SIZE {
            enc.extend_from_slice(&key[index..index + ENC_GROUP_SIZE]);
        } else {
            pad = ENC_GROUP_SIZE - remain;
            enc.extend_from_slice(&key[index..]);
            enc.extend_from_slice(&ENC_ASC_PADDING[..pad]);
        }
        enc.push(ENC_MARKER - pad as u8);
        index += ENC_GROUP_SIZE;
    }
    enc
}
