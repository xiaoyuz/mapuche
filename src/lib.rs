pub mod cmd;
pub mod frame;

mod config;
mod db;
mod gc;
mod rocks;
mod utils;

use cmd::Command;

use db::DBInner;
use frame::Frame;
use std::{path::Path, sync::Arc};

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

pub struct OpenOptions {}

impl OpenOptions {
    pub fn new() -> Self {
        OpenOptions {}
    }

    pub async fn open<P: AsRef<Path>>(self, path: P) -> Result<DB> {
        let inner = DBInner::open(path).await?;
        let inner = Arc::new(inner);
        Ok(DB { inner })
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct DB {
    pub(crate) inner: Arc<DBInner>,
}

impl DB {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        OpenOptions::new().open(path).await
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
