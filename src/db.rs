use std::sync::{atomic::AtomicU16, Arc};

use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::{
    config::{set_global_config, Config},
    rocks::{client::RocksClient, encoding::KeyEncoder, new_client},
    Result,
};

pub struct DB {
    pub(crate) inner: Arc<DBInner>,
}

impl DB {
    pub async fn new(config: Config) -> Result<Self> {
        let inner = DBInner::new(config).await?;
        let inner = Arc::new(inner);
        Ok(Self { inner })
    }
}

pub(crate) struct DBInner {
    pub(crate) client: Arc<RocksClient>,
    pub(crate) key_encoder: KeyEncoder,
    pub(crate) index_count: AtomicU16,
}

impl DBInner {
    pub(crate) async fn new(config: Config) -> Result<Self> {
        set_global_config(config);
        let client = new_client()?;
        let client = Arc::new(client);
        let key_encoder = KeyEncoder::new();
        let index_count = AtomicU16::new(SmallRng::from_entropy().gen_range(0..u16::MAX));
        Ok(Self {
            client,
            key_encoder,
            index_count,
        })
    }
}
