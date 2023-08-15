pub mod client;
pub mod cmd;
pub mod config;
pub mod frame;
pub mod gc;
pub mod rocks;
pub mod server;
pub mod utils;

mod connection;
mod db;
mod parse;
mod shutdown;

pub use cmd::Command;
pub use connection::Connection;
pub use frame::Frame;

use parse::{Parse, ParseError};
use shutdown::Shutdown;
use thiserror::Error;

/// Default port that a redis server listens on.
///
/// Used if no port is specified.
pub const DEFAULT_PORT: &str = "6380";

/// Error returned by most functions.
///
/// When writing a real application, one might want to consider a specialized
/// error handling crate or defining an error type as an `enum` of causes.
/// However, for our example, using a boxed `std::error::Error` is sufficient.
///
/// For performance reasons, boxing is avoided in any hot path. For example, in
/// `parse`, a custom error `enum` is defined. This is because the error is hit
/// and handled during normal execution when a partial frame is received on a
/// socket. `std::error::Error` is implemented for `parse::Error` which allows
/// it to be converted to `Box<dyn std::error::Error>`.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Error, Debug)]
pub enum MapucheError {
    #[error("{0}")]
    String(&'static str),
    #[error("{0}")]
    Owned(String),
}

#[derive(Debug, Clone)]
pub enum MapucheInfra {
    Single,
    Replica,
    Cluster,
}

impl MapucheInfra {
    pub fn need_raft(&self) -> bool {
        matches!(self, MapucheInfra::Replica | MapucheInfra::Cluster)
    }
}

impl From<&str> for MapucheInfra {
    fn from(value: &str) -> Self {
        match value {
            "replica" => Self::Replica,
            "cluster" => Self::Cluster,
            _ => Self::Replica,
        }
    }
}

/// A specialized `Result` type for mapuche operations.
///
/// This is defined as a convenience.
pub type Result<T> = anyhow::Result<T, Error>;
