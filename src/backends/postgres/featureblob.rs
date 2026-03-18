use serde::Deserialize;
use tokio_postgres::types::Type;

use crate::backend::{Backend, Batch};
use crate::config::{BackendConfig, BenchConfig};

use super::PgContainer;

#[derive(Debug, Clone, Deserialize)]
pub struct PgFeatureBlobConfig {
    pub image: String,
}

impl BackendConfig for PgFeatureBlobConfig {}

#[derive(Clone)]
pub struct PgFeatureBlob {
    pg: PgContainer,
}

impl Backend for PgFeatureBlob {
    type Config = PgFeatureBlobConfig;
    type Response = Vec<tokio_postgres::Row>;

    async fn init(config: &BenchConfig<Self::Config>) -> Self {
        let pg = PgContainer::start(&config.backend.image).await;
        pg.client
            .execute(
                "CREATE TABLE bench (key TEXT PRIMARY KEY, value BYTEA NOT NULL)",
                &[],
            )
            .await
            .expect("failed to create table");
        PgFeatureBlob { pg }
    }

    async fn write_batch(&self, batch: &Batch) {
        let value_cols = batch.value_columns();
        let num_cols = value_cols.len();
        let num_rows = batch.keys.len();

        // Build concrete typed vectors to avoid dyn ToSql lifetime issues.
        let mut keys_vec: Vec<String> = Vec::with_capacity(num_rows);
        let mut blobs_vec: Vec<Vec<u8>> = Vec::with_capacity(num_rows);

        for (row, key) in batch.keys.iter().enumerate() {
            let mut blob = Vec::with_capacity(num_cols * 4);
            for col in &value_cols {
                blob.extend_from_slice(&col.value(row).to_le_bytes());
            }
            keys_vec.push(key.clone());
            blobs_vec.push(blob);
        }

        let mut placeholders = Vec::with_capacity(num_rows);
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            Vec::with_capacity(num_rows * 2);

        for i in 0..num_rows {
            placeholders.push(format!("(${}, ${})", i * 2 + 1, i * 2 + 2));
            params.push(&keys_vec[i]);
            params.push(&blobs_vec[i]);
        }

        let sql = format!(
            "INSERT INTO bench (key, value) VALUES {}",
            placeholders.join(", ")
        );
        self.pg.client.execute(&sql, &params).await.unwrap();
    }

    async fn flush(&self) {
        self.pg.client.execute("CHECKPOINT", &[]).await.expect("checkpoint failed");
    }

    async fn read(&self, keys: &[String], _columns: &[String]) -> Self::Response {
        self.pg
            .client
            .query_typed(
                "SELECT key, value FROM bench WHERE key = ANY($1)",
                &[(&keys, Type::TEXT_ARRAY)],
            )
            .await
            .unwrap()
    }

    async fn memory_usage(&self) -> crate::backend::MemoryUsage {
        crate::stats::mem::MemoryUsage::for_container(self.pg._container.id()).await
    }

    async fn disk_usage(&self) -> crate::backend::DiskUsage {
        self.pg.disk_usage().await
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
            backend: PgFeatureBlobConfig {
                image: "postgres:17".to_string(),
            },
        };
        test_backend_roundtrip::<PgFeatureBlob>(config).await;
    }
}
