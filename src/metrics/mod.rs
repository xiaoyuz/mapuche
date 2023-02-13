mod http;

use prometheus::{
    exponential_buckets, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
};

use lazy_static::lazy_static;
pub use self::http::PrometheusServer;

lazy_static! {
    pub static ref INSTANCE_ID_GAUGER: IntGauge =
        register_int_gauge!("redis_instance_id", "Instance ID").unwrap();
    pub static ref TIKV_CLIENT_RETRIES: IntGauge =
        register_int_gauge!("redis_client_retries_total", "Client retries").unwrap();
    pub static ref TOTAL_CONNECTION_PROCESSED: IntCounter = register_int_counter!(
        "redis_total_connection_processed_total",
        "Total connection processed"
    )
    .unwrap();
    pub static ref DATA_TRAFFIC_OUT: IntCounter =
        register_int_counter!("redis_data_traffic_out_bytes", "Output data traffic").unwrap();
    pub static ref DATA_TRAFFIC_IN: IntCounter =
        register_int_counter!("redis_data_traffic_in_bytes", "Input data traffic").unwrap();
    pub static ref REQUEST_COUNTER: IntCounter =
        register_int_counter!("redis_requests_total", "Request counter").unwrap();
    pub static ref CURRENT_CONNECTION_COUNTER: IntGauge = register_int_gauge!(
        "redis_current_connections",
        "Current connection counter"
    )
    .unwrap();
    pub static ref REQUEST_CMD_COUNTER: IntCounterVec = register_int_counter_vec!(
        "redis_command_requests_total",
        "Request command counter",
        &["cmd"]
    )
    .unwrap();
    pub static ref REQUEST_CMD_FINISH_COUNTER: IntCounterVec = register_int_counter_vec!(
        "redis_command_requests_finish_total",
        "Request command finish counter",
        &["cmd"]
    )
    .unwrap();
    pub static ref REQUEST_CMD_ERROR_COUNTER: IntCounterVec = register_int_counter_vec!(
        "redis_command_errors_total",
        "Request command error counter",
        &["cmd"]
    )
    .unwrap();
    pub static ref REQUEST_CMD_HANDLE_TIME: HistogramVec = register_histogram_vec!(
        "redis_command_handle_time_duration_seconds",
        "Bucketed histogram of command handle duration",
        &["cmd"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref REMOVED_EXPIRED_KEY_COUNTER: IntCounterVec = register_int_counter_vec!(
        "redis_removed_expired_keys_count_total",
        "The number of expired keys that have been removed",
        &["kind"]
    )
    .unwrap();

    // Trasactions
    pub static ref SNAPSHOT_COUNTER: IntCounter = register_int_counter!("redis_snapshot_count_total", "Snapshot count").unwrap();
    pub static ref TXN_COUNTER: IntCounter = register_int_counter!("redis_txn_count_total", "Transactions count").unwrap();
    pub static ref TXN_MECHANISM_COUNTER: IntCounterVec = register_int_counter_vec!(
        "redis_txn_mechanism_count_total",
        "Transaction mechanism count",
        &["mechanism", "async"]
    )
    .unwrap();
    pub static ref TXN_DURATION: Histogram = register_histogram!(
        "redis_transaction_duration_seconds",
        "Bucketed histogram of transaction duration",
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref ACQUIRE_LOCK_DURATION: Histogram = register_histogram!(
        "redis_acquire_pessimistic_lock_duration_seconds",
        "Bucketed histogram of acquiring pessimistic lock duration",
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref RETRIEVE_TSO_DURATION: Histogram = register_histogram!(
        "redis_retrieve_tso_duration_seconds",
        "Bucketed histogram of retrieving TSO duration",
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
   pub static ref HANDLE_SNAPSHOT_DURATION: Histogram = register_histogram!(
        "redis_handle_snapshot_duration_seconds",
        "Bucketed histogram of handling snapshot duration",
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();

    pub static ref TIKV_ERR_COUNTER: IntCounterVec = register_int_counter_vec!(
        "redis_tikv_reported_errors_count_total",
        "TiKV reported err",
        &["err"]
    )
    .unwrap();

    // GC
    pub static ref GC_TASK_QUEUE_COUNTER: IntGaugeVec = register_int_gauge_vec!(
        "redis_gc_task_queue_count",
        "GC task queue gauge",
        &["worker"]
    )
    .unwrap();
}