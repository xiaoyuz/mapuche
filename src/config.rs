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

#[allow(dead_code)]
pub fn async_gc_worker_queue_size_or_default() -> usize {
    // default async gc worker queue size
    100000
}

#[allow(dead_code)]
pub fn async_gc_interval_or_default() -> u64 {
    // default async gc interval in ms
    10000
}

#[allow(dead_code)]
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

pub fn txn_retry_count() -> u32 {
    // default to 3
    10
}
