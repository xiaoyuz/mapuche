#[macro_use]
extern crate prometheus;

pub mod client;

pub mod config;

pub mod cmd;

use std::sync::atomic::{AtomicU16, Ordering};
use lazy_static::lazy_static;
use rand::{rngs::SmallRng, Rng, SeedableRng};
pub use cmd::Command;

mod connection;

pub use connection::Connection;

pub mod frame;

pub use frame::Frame;

mod db;

use db::Db;
use db::DbDropGuard;

mod parse;

use parse::{Parse, ParseError};

pub mod server;

mod buffer;

pub use buffer::{buffer, Buffer};

mod shutdown;
pub mod rocks;
pub mod utils;
pub mod metrics;

use shutdown::Shutdown;

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

/// A specialized `Result` type for mapuche operations.
///
/// This is defined as a convenience.
pub type Result<T> = std::result::Result<T, Error>;

lazy_static! {
    pub static ref INDEX_COUNT: AtomicU16 =
        AtomicU16::new(SmallRng::from_entropy().gen_range(0..u16::MAX));
}

pub fn fetch_idx_and_add() -> u16 {
    // fetch_add wraps around on overflow, see https://github.com/rust-lang/rust/issues/34618
    INDEX_COUNT.fetch_add(1, Ordering::Relaxed)
}
