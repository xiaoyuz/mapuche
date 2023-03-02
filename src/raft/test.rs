// use std::future::Future;
// use std::sync::Arc;
// use openraft::async_trait::async_trait;
// use openraft::{StorageError};
// use openraft::testing::{StoreBuilder};
// use crate::raft::{MapucheNodeId, RocksStore, TypeConfig};
//
// struct RocksBuilder {}
//
// #[async_trait]
// impl StoreBuilder<TypeConfig, Arc<RocksStore>> for RocksBuilder {
//     async fn run_test<Fun, Ret, Res>(&self, t: Fun) -> Result<Ret, StorageError<MapucheNodeId>>
//         where
//             Res: Future<Output = Result<Ret, StorageError<MapucheNodeId>>> + Send,
//             Fun: Fn(Arc<RocksStore>) -> Res + Sync + Send,
//     {
//         let td = tempdir::TempDir::new("RocksBuilder").expect("couldn't create temp dir");
//         let r = {
//             let store = RocksStore::new(td.path()).await;
//             t(store).await
//         };
//         td.close().expect("could not close temp directory");
//         r
//     }
// }
//
// #[test]
// pub fn test_mem_store() -> Result<(), StorageError<MapucheNodeId>> {
//     Suite::test_all(RocksBuilder {})?;
//     Ok(())
// }
