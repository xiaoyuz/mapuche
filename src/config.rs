use crate::DEFAULT_PORT;

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    server: Server,
    backend: Backend,
}

#[derive(Debug, Deserialize, Clone)]
struct Server {
    listen: Option<String>,
    port: Option<u16>,
    log_level: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct Backend {
    data_store_dir: Option<String>,
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
    format!("{}/{}", data_store_dir_or_default(), "/rocksdb-service.log")
}

pub fn config_meta_key_number_or_default() -> u16 {
    // default metakey split number
    u16::MAX
}

pub fn async_expire_set_threshold_or_default() -> u32 {
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_deletion_enabled_or_default() -> bool {
    // default async deletion enabled
    false
}

pub fn async_del_set_threshold_or_default() -> u32 {
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_gc_worker_queue_size_or_default() -> usize {
    // default async gc worker queue size
    100000
}

pub fn async_gc_interval_or_default() -> u64 {
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
    10
}

pub fn async_del_list_threshold_or_default() -> u32 {
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn cmd_linsert_length_limit_or_default() -> u32 {
    // default linsert length no limit
    0
}

pub fn cmd_lrem_length_limit_or_default() -> u32 {
    // default lrem length no limit
    0
}

pub fn async_expire_list_threshold_or_default() -> u32 {
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_expire_hash_threshold_or_default() -> u32 {
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_del_hash_threshold_or_default() -> u32 {
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_expire_zset_threshold_or_default() -> u32 {
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_del_zset_threshold_or_default() -> u32 {
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn config_local_pool_number() -> usize {
    // default use 8 localset pool to handle connections
    8
}

pub fn config_max_connection() -> usize {
    // default use 8 localset pool to handle connections
    10000
}

pub fn txn_retry_count() -> u32 {
    // default to 3
    10
}
