pub mod cmd;
pub mod config;
pub mod frame;

mod db;
mod gc;
mod parse;
mod rocks;
mod shutdown;
mod utils;

use cmd::Command;
use config::Config;

use db::DBInner;
use frame::Frame;
use parse::Parse;
use std::sync::Arc;

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

/// A specialized `Result` type for mapuche operations.
///
/// This is defined as a convenience.
pub type Result<T> = anyhow::Result<T, Error>;

pub struct DB {
    pub(crate) inner: Arc<DBInner>,
}

impl DB {
    pub async fn new(config: Config) -> Result<Self> {
        let inner = DBInner::new(config).await?;
        let inner = Arc::new(inner);
        Ok(Self { inner })
    }

    pub fn conn(&self) -> Conn {
        Conn {
            inner: self.inner.clone(),
        }
    }
}

pub struct Conn {
    pub(crate) inner: Arc<DBInner>,
}

impl Conn {
    pub async fn execute(&self, cmd: Command) -> crate::Result<Frame> {
        cmd.execute(&self.inner).await
    }
}
