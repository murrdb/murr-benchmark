use serde::Deserialize;
use tokio_postgres::types::Type;

use crate::backend::{Backend, Batch};
use crate::config::{BackendConfig, BenchConfig};

use super::PgContainer;

#[derive(Debug, Clone, Deserialize)]
pub struct PgFeastConfig {
    pub image: String,
}

impl BackendConfig for PgFeastConfig {}

#[derive(Clone)]
pub struct PgFeast {
    pg: PgContainer,
}

impl Backend for PgFeast {
    type Config = PgFeastConfig;
    type Response = Vec<tokio_postgres::Row>;

    async fn init(config: &BenchConfig<Self::Config>) -> Self {
        let pg = PgContainer::start(&config.backend.image).await;

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

        PgFeast { pg }
    }

    async fn write_batch(&self, batch: &Batch) {
        let value_cols = batch.value_columns();
        let num_rows = batch.keys.len();
        let params_per_row = 1 + value_cols.len();

        // Pre-extract all f32 values into a flat vec so we can reference them.
        let mut float_values: Vec<f32> = Vec::with_capacity(num_rows * value_cols.len());
        for row in 0..num_rows {
            for col in &value_cols {
                float_values.push(col.value(row));
            }
        }

        let mut placeholders = Vec::with_capacity(num_rows);
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            Vec::with_capacity(num_rows * params_per_row);

        for (row, key) in batch.keys.iter().enumerate() {
            let base = row * params_per_row;
            let row_placeholders: Vec<String> = (0..params_per_row)
                .map(|i| format!("${}", base + i + 1))
                .collect();
            placeholders.push(format!("({})", row_placeholders.join(", ")));

            params.push(key);
            let float_offset = row * value_cols.len();
            for i in 0..value_cols.len() {
                params.push(&float_values[float_offset + i]);
            }
        }

        let col_names: Vec<String> = (0..value_cols.len()).map(|i| format!("col_{i}")).collect();
        let sql = format!(
            "INSERT INTO bench (key, {}) VALUES {}",
            col_names.join(", "),
            placeholders.join(", ")
        );
        self.pg.client.execute(&sql, &params).await.unwrap();
    }

    async fn read(&self, keys: &[String], columns: &[String]) -> Self::Response {
        let col_list = columns.join(", ");
        let sql = format!("SELECT {col_list} FROM bench WHERE key = ANY($1)");
        self.pg
            .client
            .query_typed(&sql, &[(&keys, Type::TEXT_ARRAY)])
            .await
            .unwrap()
    }

    async fn memory_usage(&self) -> crate::backend::MemoryUsage {
        crate::stats::mem::MemoryUsage::for_container(self.pg._container.id()).await
    }

    async fn disk_usage(&self) -> crate::backend::DiskUsage {
        crate::stats::disk::DiskUsage::for_container(self.pg._container.id()).await
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
                image: "postgres:17".to_string(),
            },
        };
        test_backend_roundtrip::<PgFeast>(config).await;
    }
}
