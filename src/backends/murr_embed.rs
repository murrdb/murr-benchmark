use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use serde::Deserialize;
use tempfile::TempDir;

use murr::conf::{Config, StorageConfig};
use murr::core::{ColumnSchema, DType, TableSchema};
use murr::service::MurrService;

use crate::backend::{Backend, Batch};
use crate::config::{BackendConfig, BenchConfig};
use crate::testdata;

#[derive(Debug, Clone, Deserialize)]
pub struct MurrEmbedConfig {}

impl BackendConfig for MurrEmbedConfig {}

#[derive(Clone)]
pub struct MurrEmbed {
    svc: Arc<MurrService>,
    _dir: Arc<TempDir>,
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
        let dir = TempDir::new().unwrap();
        let murr_config = Config {
            storage: StorageConfig {
                cache_dir: dir.path().to_path_buf(),
            },
            ..Config::default()
        };

        let svc = MurrService::new(murr_config).await.unwrap();
        let table_schema = Self::build_table_schema(config.select_cols);
        svc.create("bench", table_schema).await.unwrap();

        MurrEmbed {
            svc: Arc::new(svc),
            _dir: Arc::new(dir),
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

    async fn cleanup(self) {
        drop(self.svc);
        drop(self._dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BenchConfig;
    use crate::testing::test_backend_roundtrip;

    #[tokio::test]
    async fn roundtrip() {
        let config = BenchConfig {
            total_rows: 100,
            select_rows: 10,
            select_cols: 2,
            write_batch_size: 50,
            measurement_time_secs: 1,
            warmup_time_secs: 1,
            sample_size: 1,
            backend: MurrEmbedConfig {},
        };
        test_backend_roundtrip::<MurrEmbed>(config).await;
    }
}
