#[macro_use]
extern crate prometheus;

pub mod client;

pub mod config;

pub mod cmd;

pub use cmd::Command;
use lazy_static::lazy_static;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::sync::atomic::{AtomicU16, Ordering};

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

pub mod gc;
pub mod hash_ring;
pub mod metrics;
pub mod p2p;
pub mod raft;
pub mod rocks;
mod shutdown;
pub mod utils;

use crate::p2p::client::P2PClient;
use shutdown::Shutdown;

use crate::hash_ring::{HashRing, NodeInfo};
use crate::raft::client::RaftClient;
use thiserror::Error;

/// Default port that a redis server listens on.
///
/// Used if no port is specified.
pub const DEFAULT_PORT: &str = "6380";
pub const DEFAULT_RING_PORT: &str = "6123";
pub const DEFAULT_RAFT_PORT: &str = "16123";

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

lazy_static! {
    pub static ref INDEX_COUNT: AtomicU16 =
        AtomicU16::new(SmallRng::from_entropy().gen_range(0..u16::MAX));
}

pub static mut P2P_CLIENT: Option<P2PClient> = None;
pub static mut RING_NODES: Option<HashRing<NodeInfo>> = None;
pub static mut RAFT_CLIENT: Option<RaftClient> = None;

pub fn fetch_idx_and_add() -> u16 {
    // fetch_add wraps around on overflow, see https://github.com/rust-lang/rust/issues/34618
    INDEX_COUNT.fetch_add(1, Ordering::Relaxed)
}
