use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use serde::Deserialize;

use murr::conf::{Config, StorageConfig};
use murr::core::{ColumnSchema, DType, TableSchema};
use murr::service::MurrService;

use crate::backend::{Backend, Batch};
use crate::config::{BackendConfig, BenchConfig};
use crate::testdata;

#[derive(Debug, Clone, Deserialize)]
pub struct MurrEmbedConfig {
    pub data_dir: PathBuf,
}

impl BackendConfig for MurrEmbedConfig {}

#[derive(Clone)]
pub struct MurrEmbed {
    svc: Arc<MurrService>,
    data_dir: PathBuf,
}

impl MurrEmbed {
    fn build_table_schema(num_cols: usize) -> TableSchema {
        let mut columns = HashMap::new();
        columns.insert(
            "key".to_string(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        for name in testdata::column_names(num_cols) {
            columns.insert(
                name,
                ColumnSchema {
                    dtype: DType::Float32,
                    nullable: false,
                },
            );
        }
        TableSchema {
            key: "key".to_string(),
            columns,
        }
    }
}

impl Backend for MurrEmbed {
    type Config = MurrEmbedConfig;
    type Response = RecordBatch;

    async fn init(config: &BenchConfig<Self::Config>) -> Self {
        let data_dir = &config.backend.data_dir;
        std::fs::create_dir_all(data_dir).expect("failed to create data_dir");

        let murr_config = Config {
            storage: StorageConfig {
                cache_dir: data_dir.clone(),
            },
            ..Config::default()
        };

        let svc = MurrService::new(murr_config).await.unwrap();
        let table_schema = Self::build_table_schema(config.select_cols);
        svc.create("bench", table_schema).await.unwrap();

        MurrEmbed {
            svc: Arc::new(svc),
            data_dir: data_dir.clone(),
        }
    }

    async fn write_batch(&self, batch: &Batch) {
        self.svc.write("bench", &batch.inner).await.unwrap();
    }

    async fn read(&self, keys: &[String], columns: &[String]) -> Self::Response {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        self.svc.read("bench", &key_refs, &col_refs).await.unwrap()
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
            backend: MurrEmbedConfig {
                data_dir: dir.path().to_path_buf(),
            },
        };
        test_backend_roundtrip::<MurrEmbed>(config).await;
    }
}
