use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use arrow::record_batch::RecordBatch;
use serde::Deserialize;

use murr::conf::{BackendConfig as StorageBackend, Config, StorageConfig};
use murr::core::{FetchRequest, TableSchema};
use murr::io::store::rocksdb::RocksDBStore;
use murr::service::MurrService;

use crate::backend::Backend;
use crate::codec::arrow::{ArrowBatch, ArrowKeys};
use crate::codec::{BatchEncoder, KeySetEncoder};
use crate::config::{BackendConfig, DbConfig};
use crate::workload::{Request, RowBatch, Schema};

#[derive(Debug, Clone, Deserialize)]
pub struct MurrEmbedConfig {
    pub data_dir: PathBuf,
    #[serde(default, flatten)]
    pub storage: StorageBackend,
}

impl BackendConfig for MurrEmbedConfig {}

#[derive(Clone)]
pub struct MurrEmbed {
    svc: Arc<MurrService<RocksDBStore>>,
    data_dir: PathBuf,
    rows: ArrowBatch,
    keys: ArrowKeys,
}

impl Backend for MurrEmbed {
    type Config = MurrEmbedConfig;
    type Response = RecordBatch;

    async fn init(config: &DbConfig<Self::Config>, schema: Arc<Schema>) -> Self {
        let data_dir = &config.backend.data_dir;
        std::fs::create_dir_all(data_dir).expect("failed to create data_dir");

        let murr_config = Config {
            storage: StorageConfig {
                path: data_dir.clone(),
                backend: config.backend.storage.clone(),
            },
            ..Config::default()
        };

        let store = RocksDBStore::open_from_config(&murr_config.storage).unwrap();
        let svc = MurrService::new(Arc::new(RwLock::new(store)), murr_config).unwrap();
        svc.create("bench", TableSchema::from(schema.as_ref())).unwrap();

        MurrEmbed {
            svc: Arc::new(svc),
            data_dir: data_dir.clone(),
            rows: ArrowBatch::new(schema.clone()),
            keys: ArrowKeys::new(schema),
        }
    }

    async fn write_batch(&self, batch: &RowBatch) {
        self.svc.write("bench", &self.rows.encode(batch)).unwrap();
    }

    async fn read(&self, request: &Request) -> Self::Response {
        let fetch = FetchRequest {
            keys: self.keys.encode(&request.keys),
            columns: request.columns.clone(),
        };
        self.svc.read("bench", &fetch).unwrap()
    }

    async fn flush(&self) {
        self.svc.compact("bench").unwrap();
    }

    async fn memory_usage(&self) -> crate::backend::MemoryUsage {
        crate::stats::mem::MemoryUsage::for_process()
    }

    async fn disk_usage(&self) -> crate::backend::DiskUsage {
        crate::stats::disk::DiskUsage::for_path(&self.data_dir)
    }

    async fn cleanup(self) {
        drop(self.svc);
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DbConfig;
    use crate::testing::test_backend_roundtrip;
    use murr::io::store::rocksdb::block::BlockConfig;
    use murr::io::store::rocksdb::plain::PlainConfig;
    use tempfile::TempDir;

    fn base_config(dir: &TempDir, storage: StorageBackend) -> DbConfig<MurrEmbedConfig> {
        DbConfig {
            write_batch_size: 50,
            measurement_time_secs: 1,
            warmup_time_secs: 1,
            sample_size: 1,
            backend: MurrEmbedConfig {
                data_dir: dir.path().to_path_buf(),
                storage,
            },
        }
    }

    #[tokio::test]
    async fn roundtrip_mmap() {
        let dir = TempDir::new().unwrap();
        let config = base_config(&dir, StorageBackend::Mmap(PlainConfig::default()));
        test_backend_roundtrip::<MurrEmbed>(config).await;
    }

    #[tokio::test]
    async fn roundtrip_block() {
        let dir = TempDir::new().unwrap();
        let config = base_config(&dir, StorageBackend::Block(BlockConfig::default()));
        test_backend_roundtrip::<MurrEmbed>(config).await;
    }
}
