use std::num::{ParseFloatError, ParseIntError};
use thiserror::Error;
use rocksdb::Error as RocksError;

#[derive(Error, Debug)]
pub enum RError {
    #[error("{0}")]
    RocksClient(Box<RocksError>),
    #[error("{0}")]
    String(&'static str),
    #[error("{0}")]
    Owned(String),
}

impl RError {
    pub fn owned_error<T>(s: T) -> Self
        where T: Into<String>
    {
        RError::Owned(s.into())
    }

    pub fn is_not_integer_error<E>(_: E) -> RError {
        REDIS_VALUE_IS_NOT_INTEGER_ERR
    }
}

impl From<RocksError> for RError {
    fn from(e: RocksError) -> Self {
        RError::RocksClient(Box::new(e))
    }
}

impl From<&'static str> for RError {
    fn from(e: &'static str) -> Self {
        RError::String(e)
    }
}

impl From<ParseIntError> for RError {
    fn from(_: ParseIntError) -> Self {
        REDIS_VALUE_IS_NOT_INTEGER_ERR
    }
}

impl From<ParseFloatError> for RError {
    fn from(_: ParseFloatError) -> Self {
        REDIS_VALUE_IS_NOT_VALID_FLOAT_ERR
    }
}

pub const REDIS_WRONG_TYPE_ERR: RError =
    RError::String("WRONGTYPE Operation against a key holding the wrong kind of value");
pub const REDIS_VALUE_IS_NOT_INTEGER_ERR: RError =
    RError::String("ERR value is not an integer or out of range");
pub const REDIS_VALUE_IS_NOT_VALID_FLOAT_ERR: RError =
    RError::String("ERR value is not a valid float");
pub const REDIS_BACKEND_NOT_CONNECTED_ERR: RError = RError::String("ERR backend not connected");
pub const REDIS_COMPARE_AND_SWAP_EXHAUSTED_ERR: RError =
    RError::String("ERR compare-and-swap exhausted");
pub const DECREMENT_OVERFLOW: RError = RError::String("Decrement would overflow");
pub const TXN_ERROR: RError = RError::String("Txn commit failed");
pub const KEY_VERSION_EXHUSTED_ERR: RError = RError::String("ERR key version exhausted");
pub const CF_NOT_EXISTS_ERR: RError = RError::String("Column family not existed");
