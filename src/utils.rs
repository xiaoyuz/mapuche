use crate::rocks::errors::RError;
use crate::Frame;
use std::collections::HashSet;
use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const TIMESTAMP_FORMAT: &str = "%Y/%m/%d %H:%M:%S%.3f %:z";

pub fn resp_ok() -> Frame {
    Frame::Simple("OK".to_string())
}

pub fn resp_str(val: &str) -> Frame {
    Frame::Simple(val.to_string())
}

pub fn resp_invalid_arguments() -> Frame {
    Frame::Error("Invalid arguments".to_string())
}

pub fn resp_nil() -> Frame {
    Frame::Null
}

pub fn resp_err(e: RError) -> Frame {
    e.into()
}

pub fn resp_bulk(val: Vec<u8>) -> Frame {
    Frame::Bulk(val.into())
}

pub fn resp_array(val: Vec<Frame>) -> Frame {
    Frame::Array(val)
}

pub fn resp_int(val: i64) -> Frame {
    Frame::Integer(val)
}

pub fn timestamp_from_ttl(ttl: i64) -> i64 {
    ttl + now_timestamp_in_millis()
}

pub fn now_timestamp_in_millis() -> i64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    (d.as_secs() * 1000 + d.subsec_millis() as u64) as i64
}

pub fn key_is_expired(ttl: i64) -> bool {
    if ttl < 0 {
        return false;
    }
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let ts = (d.as_secs() * 1000 + d.subsec_millis() as u64) as i64;
    ttl > 0 && ttl < ts
}

pub fn ttl_from_timestamp(timestamp: i64) -> i64 {
    let now = now_timestamp_in_millis();
    if now > timestamp {
        0
    } else {
        timestamp - now
    }
}

#[allow(dead_code)]
pub async fn sleep(ms: u32) {
    tokio::time::sleep(Duration::from_millis(ms as u64)).await;
}

pub fn count_unique_keys<T: std::hash::Hash + Eq>(keys: &[T]) -> usize {
    keys.iter().collect::<HashSet<&T>>().len()
}

#[allow(dead_code)]
pub fn timestamp_local(io: &mut dyn io::Write) -> io::Result<()> {
    let now = chrono::Local::now().format(TIMESTAMP_FORMAT);
    write!(io, "{now}")
}
