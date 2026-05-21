use std::sync::Arc;

use serde::Deserialize;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};

use crate::backend::{Backend, Batch};
use crate::config::{BackendConfig, BenchConfig};

use super::{
    PgContainer, default_effective_cache_size, default_shared_buffers, default_work_mem,
};

#[derive(Debug, Clone, Deserialize)]
pub struct PgFeastConfig {
    pub image: String,
    #[serde(default)]
    pub cgroup_memory_mb: Option<i64>,
    #[serde(default = "default_shared_buffers")]
    pub shared_buffers: String,
    #[serde(default = "default_work_mem")]
    pub work_mem: String,
    #[serde(default = "default_effective_cache_size")]
    pub effective_cache_size: String,
}

impl BackendConfig for PgFeastConfig {}

#[derive(Clone)]
pub struct PgFeast {
    pg: PgContainer,
    read_stmt: Arc<tokio_postgres::Statement>,
}

impl Backend for PgFeast {
    type Config = PgFeastConfig;
    type Response = Vec<tokio_postgres::Row>;

    async fn init(config: &BenchConfig<Self::Config>) -> Self {
        let pg = PgContainer::start(
            &config.backend.image,
            config.backend.cgroup_memory_mb,
            &config.backend.shared_buffers,
            &config.backend.work_mem,
            &config.backend.effective_cache_size,
        )
        .await;

        let col_defs: Vec<String> = (0..config.select_cols)
            .map(|i| format!("col_{i} REAL NOT NULL"))
            .collect();
        let ddl = format!(
            "CREATE TABLE bench (key TEXT PRIMARY KEY, {})",
            col_defs.join(", ")
        );
        pg.client
            .execute(&ddl, &[])
            .await
            .expect("failed to create table");

        let col_list: String = (0..config.select_cols)
            .map(|i| format!("col_{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let read_sql = format!("SELECT {col_list} FROM bench WHERE key = ANY($1)");
        let read_stmt = pg
            .client
            .prepare_typed(&read_sql, &[Type::TEXT_ARRAY])
            .await
            .expect("failed to prepare read statement");

        PgFeast {
            pg,
            read_stmt: Arc::new(read_stmt),
        }
    }

    async fn write_batch(&self, batch: &Batch) {
        let value_cols = batch.value_columns();
        let num_cols = value_cols.len();
        let num_rows = batch.keys.len();

        // Flatten f32 values so we can take stable references into the BinaryCopyInWriter.
        let mut float_values: Vec<f32> = Vec::with_capacity(num_rows * num_cols);
        for row in 0..num_rows {
            for col in &value_cols {
                float_values.push(col.value(row));
            }
        }

        let col_names: Vec<String> = (0..num_cols).map(|i| format!("col_{i}")).collect();
        let mut types: Vec<Type> = Vec::with_capacity(1 + num_cols);
        types.push(Type::TEXT);
        types.extend(std::iter::repeat_n(Type::FLOAT4, num_cols));

        let sql = format!(
            "COPY bench (key, {}) FROM STDIN BINARY",
            col_names.join(", ")
        );
        let sink = self.pg.client.copy_in(&sql).await.unwrap();
        let writer = BinaryCopyInWriter::new(sink, &types);
        tokio::pin!(writer);

        let mut row_refs: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(1 + num_cols);
        for (row, key) in batch.keys.iter().enumerate() {
            row_refs.clear();
            row_refs.push(key);
            let float_offset = row * num_cols;
            for i in 0..num_cols {
                row_refs.push(&float_values[float_offset + i]);
            }
            writer.as_mut().write(&row_refs).await.unwrap();
        }
        writer.finish().await.unwrap();
    }

    async fn flush(&self) {
        self.pg
            .client
            .execute("ANALYZE bench", &[])
            .await
            .expect("analyze failed");
        self.pg
            .client
            .execute("CHECKPOINT", &[])
            .await
            .expect("checkpoint failed");
    }

    async fn read(&self, keys: &[String], _columns: &[String]) -> Self::Response {
        self.pg
            .client
            .query(&*self.read_stmt, &[&keys])
            .await
            .unwrap()
    }

    async fn memory_usage(&self) -> crate::backend::MemoryUsage {
        crate::stats::mem::MemoryUsage::for_container(self.pg._container.id()).await
    }

    async fn disk_usage(&self) -> crate::backend::DiskUsage {
        self.pg.disk_usage().await
    }

    async fn network_usage(&self) -> crate::backend::NetworkUsage {
        crate::stats::net::NetworkUsage::for_container(self.pg._container.id()).await
    }

    async fn cleanup(self) {
        drop(self.pg);
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
            backend: PgFeastConfig {
                image: "postgres:18.3".to_string(),
                cgroup_memory_mb: None,
                shared_buffers: default_shared_buffers(),
                work_mem: default_work_mem(),
                effective_cache_size: default_effective_cache_size(),
            },
        };
        test_backend_roundtrip::<PgFeast>(config).await;
    }
}
