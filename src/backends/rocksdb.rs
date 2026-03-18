use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;

use crate::backend::{Backend, Batch};
use crate::config::{BackendConfig, BenchConfig};

#[derive(Debug, Clone, Deserialize)]
pub struct RocksDbConfig {
    pub data_dir: PathBuf,
}

impl BackendConfig for RocksDbConfig {}

#[derive(Clone)]
pub struct RocksDb {
    db: Arc<rocksdb::DB>,
    data_dir: PathBuf,
}

impl Backend for RocksDb {
    type Config = RocksDbConfig;
    type Response = Vec<Option<Vec<u8>>>;

    async fn init(config: &BenchConfig<Self::Config>) -> Self {
        let data_dir = &config.backend.data_dir;
        std::fs::create_dir_all(data_dir).expect("failed to create data_dir");

        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = rocksdb::DB::open(&opts, data_dir).expect("failed to open RocksDB");

        RocksDb {
            db: Arc::new(db),
            data_dir: data_dir.clone(),
        }
    }

    async fn write_batch(&self, batch: &Batch) {
        let value_cols = batch.value_columns();
        let mut wb = rocksdb::WriteBatch::default();

        for (row, key) in batch.keys.iter().enumerate() {
            let mut blob = Vec::with_capacity(value_cols.len() * 4);
            for col in &value_cols {
                blob.extend_from_slice(&col.value(row).to_le_bytes());
            }
            wb.put(key.as_bytes(), &blob);
        }

        self.db.write(wb).expect("failed to write batch");
    }

    async fn read(&self, keys: &[String], _columns: &[String]) -> Self::Response {
        self.db
            .multi_get(keys.iter().map(|k| k.as_bytes()))
            .into_iter()
            .map(|r| r.expect("rocksdb read error"))
            .collect()
    }

    async fn memory_usage(&self) -> crate::backend::MemoryUsage {
        crate::stats::mem::MemoryUsage::for_process()
    }

    async fn disk_usage(&self) -> crate::backend::DiskUsage {
        crate::stats::disk::DiskUsage::for_path(&self.data_dir)
    }

    async fn cleanup(self) {
        drop(self.db);
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BenchConfig;
    use crate::testing::test_backend_roundtrip;
    use tempfile::TempDir;

    #[tokio::test]
    async fn roundtrip() {
        let dir = TempDir::new().unwrap();
        let config = BenchConfig {
            total_rows: 100,
            select_rows: 10,
            select_cols: 2,
            write_batch_size: 50,
            measurement_time_secs: 1,
            warmup_time_secs: 1,
            sample_size: 1,
            backend: RocksDbConfig {
                data_dir: dir.path().to_path_buf(),
            },
        };
        test_backend_roundtrip::<RocksDb>(config).await;
    }
}
