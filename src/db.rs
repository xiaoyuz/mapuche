use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicU16, Arc};

use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::{
    config::config_meta_key_number_or_default,
    rocks::{client::RocksClient, encoding::KeyEncoder, new_client},
    Result,
};

pub struct DBInner {
    pub(crate) client: Arc<RocksClient>,
    pub(crate) key_encoder: KeyEncoder,
    pub(crate) index_count: AtomicU16,
}

impl DBInner {
    pub(crate) async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let client = new_client(path)?;
        let client = Arc::new(client);
        let key_encoder = KeyEncoder::new();
        let index_count = AtomicU16::new(SmallRng::from_entropy().gen_range(0..u16::MAX));
        Ok(Self {
            client,
            key_encoder,
            index_count,
        })
    }

    pub(crate) fn gen_next_meta_index(&self) -> u16 {
        let idx = self.index_count.fetch_add(1, Ordering::Relaxed);
        idx % config_meta_key_number_or_default()
    }
}
