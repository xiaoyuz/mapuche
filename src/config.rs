use crate::{DEFAULT_PORT, DEFAULT_RING_PORT};
use lazy_static::lazy_static;
use serde::Deserialize;

use slog::{self, Drain};
use slog_term;
use std::fs::OpenOptions;

lazy_static! {
    pub static ref LOGGER: slog::Logger = slog::Logger::root(
        slog_term::FullFormat::new(slog_term::PlainSyncDecorator::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(log_file())
                .unwrap()
        ))
        .use_custom_timestamp(crate::utils::timestamp_local)
        .build()
        .filter_level(slog::Level::from_usize(log_level()).unwrap())
        .fuse(),
        slog::o!()
    );
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    server: Server,
    backend: Backend,
}

#[derive(Debug, Deserialize, Clone)]
struct Server {
    listen: Option<String>,
    port: Option<u16>,
    ring_port: Option<u16>,
    instance_id: Option<String>,
    prometheus_listen: Option<String>,
    prometheus_port: Option<u16>,
    password: Option<String>,
    log_level: Option<String>,
    log_file: Option<String>,
    meta_key_number: Option<u16>,
    cluster: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct Backend {
    local_pool_number: Option<usize>,
    max_connection: Option<usize>,

    txn_retry_count: Option<u32>,

    data_store_dir: Option<String>,

    cmd_lrem_length_limit: Option<u32>,
    cmd_linsert_length_limit: Option<u32>,

    async_deletion_enabled: Option<bool>,

    async_gc_worker_number: Option<usize>,
    async_gc_worker_queue_size: Option<usize>,
    async_gc_interval: Option<u64>,

    async_del_list_threshold: Option<u32>,
    async_del_hash_threshold: Option<u32>,
    async_del_set_threshold: Option<u32>,
    async_del_zset_threshold: Option<u32>,

    async_expire_list_threshold: Option<u32>,
    async_expire_hash_threshold: Option<u32>,
    async_expire_set_threshold: Option<u32>,
    async_expire_zset_threshold: Option<u32>,
}

// Config
pub static mut SERVER_CONFIG: Option<Config> = None;

pub fn set_global_config(config: Config) {
    unsafe {
        SERVER_CONFIG.replace(config);
    }
}

pub fn config_listen_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.listen.clone() {
                return s;
            }
        }
    }

    "0.0.0.0".to_owned()
}

pub fn config_port_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.port {
                return s.to_string();
            }
        }
    }

    DEFAULT_PORT.to_owned()
}

pub fn config_ring_port_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.ring_port {
                return s.to_string();
            }
        }
    }

    DEFAULT_RING_PORT.to_owned()
}

pub fn config_instance_id_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.instance_id.clone() {
                return s;
            }
        }
    }
    "1".to_owned()
}

pub fn config_prometheus_listen_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.prometheus_listen.clone() {
                return s;
            }
        }
    }
    "0.0.0.0".to_owned()
}

pub fn config_prometheus_port_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.prometheus_port {
                return s.to_string();
            }
        }
    }
    "18080".to_owned()
}

pub fn config_cluster_or_default() -> Vec<String> {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.cluster.clone() {
                return s.split(',').map(|s| s.to_string()).collect::<Vec<String>>();
            }
        }
    }
    Vec::default()
    // "10.7.8.73:6123"
    //     .split(',')
    //     .map(|s| s.to_string())
    //     .collect::<Vec<String>>()
}

fn log_level_str() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(l) = c.server.log_level.clone() {
                return l;
            }
        }
    }
    "info".to_owned()
}

pub fn log_level() -> usize {
    let level_str = log_level_str();
    match level_str.as_str() {
        "off" => 0,
        "critical" => 1,
        "error" => 2,
        "warning" => 3,
        "info" => 4,
        "debug" => 5,
        "trace" => 6,
        _ => 0,
    }
}

pub fn log_file() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(l) = c.server.log_file.clone() {
                return l;
            }
        }
    }
    "rocksdb-service.log".to_owned()
}

pub fn config_meta_key_number_or_default() -> u16 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.meta_key_number {
                return s;
            }
        }
    }

    // default metakey split number
    u16::MAX
}

pub fn async_expire_set_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_expire_set_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_deletion_enabled_or_default() -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_deletion_enabled {
                return b;
            }
        }
    }
    // default async deletion enabled
    true
}

pub fn async_del_set_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_del_set_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_gc_worker_queue_size_or_default() -> usize {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_gc_worker_queue_size {
                return b;
            }
        }
    }
    // default async gc worker queue size
    100000
}

pub fn async_gc_interval_or_default() -> u64 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_gc_interval {
                return b;
            }
        }
    }
    // default async gc interval in ms
    10000
}

pub fn data_store_dir_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.data_store_dir.clone() {
                return b;
            }
        }
    }
    "./mapuche_store".to_owned()
}

pub fn async_gc_worker_number_or_default() -> usize {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_gc_worker_number {
                return b;
            }
        }
    }
    // default async gc worker number
    10
}

pub fn async_del_list_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_del_list_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn cmd_linsert_length_limit_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.cmd_linsert_length_limit {
                return b;
            }
        }
    }
    // default linsert length no limit
    0
}

pub fn cmd_lrem_length_limit_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.cmd_lrem_length_limit {
                return b;
            }
        }
    }
    // default lrem length no limit
    0
}

pub fn async_expire_list_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_expire_list_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_expire_hash_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_expire_hash_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_del_hash_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_del_hash_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_expire_zset_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_expire_zset_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_del_zset_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_del_zset_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn config_local_pool_number() -> usize {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.backend.local_pool_number {
                return s;
            }
        }
    }
    // default use 8 localset pool to handle connections
    8
}

pub fn config_max_connection() -> usize {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.backend.max_connection {
                return s;
            }
        }
    }
    // default use 8 localset pool to handle connections
    10000
}

pub fn txn_retry_count() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.backend.txn_retry_count {
                return s;
            }
        }
    }
    // default to 3
    10
}

pub fn is_auth_enabled() -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if c.server.password.clone().is_some() {
                return true;
            }
        }
    }
    false
}

// return false only if auth is enabled and password mismatch
pub fn is_auth_matched(password: &str) -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.password.clone() {
                return s == password;
            }
        }
    }
    true
}
