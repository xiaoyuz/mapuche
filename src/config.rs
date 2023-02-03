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
    tls_listen: Option<String>,
    tls_port: Option<u16>,
    tls_key_file: Option<String>,
    tls_cert_file: Option<String>,
    tls_auth_client: Option<bool>,
    tls_ca_cert_file: Option<String>,
    pd_addrs: Option<String>,
    instance_id: Option<String>,
    prometheus_listen: Option<String>,
    prometheus_port: Option<u16>,
    // username: Option<String>,
    password: Option<String>,
    log_level: Option<String>,
    log_file: Option<String>,
    cluster_broadcast_addr: Option<String>,
    cluster_topology_interval: Option<u64>,
    cluster_topology_expire: Option<u64>,
    meta_key_number: Option<u16>,
}

#[derive(Debug, Deserialize, Clone)]
struct Backend {
    timeout: Option<u64>,
    ca_file: Option<String>,
    cert_file: Option<String>,
    key_file: Option<String>,
    conn_concurrency: Option<usize>,
    use_txn_api: Option<bool>,
    use_async_commit: Option<bool>,
    try_one_pc_commit: Option<bool>,
    use_pessimistic_txn: Option<bool>,
    local_pool_number: Option<usize>,

    // kv client config
    completion_queue_size: Option<usize>,
    grpc_keepalive_time: Option<u64>,
    grpc_keepalive_timeout: Option<u64>,
    allow_batch: Option<bool>,
    overload_threshold: Option<u64>,
    max_batch_wait_time: Option<u64>,
    max_batch_size: Option<usize>,
    max_inflight_requests: Option<usize>,

    txn_retry_count: Option<u32>,
    txn_region_backoff_delay_ms: Option<u64>,
    txn_region_backoff_delay_attemps: Option<u32>,
    txn_lock_backoff_delay_ms: Option<u64>,
    txn_lock_backoff_delay_attemps: Option<u32>,

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

pub fn config_meta_key_number_or_default() -> u16 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.meta_key_number {
                return s;
            }
        }
    }

    // default metakey split number
    100
}