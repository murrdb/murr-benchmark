use std::borrow::Cow;
use std::sync::Arc;

use serde::Deserialize;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};

use crate::backend::Backend;
use crate::codec::blob::BlobRow;
use crate::codec::key::StringKey;
use crate::codec::{KeyEncoder, RowEncoder};
use crate::config::{BackendConfig, DbConfig};
use crate::workload::{Request, RowBatch, Schema};

use super::{
    PgContainer, default_effective_cache_size, default_shared_buffers, default_work_mem,
};

#[derive(Debug, Clone, Deserialize)]
pub struct PgFeatureBlobConfig {
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

impl BackendConfig for PgFeatureBlobConfig {}

#[derive(Clone)]
pub struct PgFeatureBlob {
    pg: PgContainer,
    read_stmt: Arc<tokio_postgres::Statement>,
    key: StringKey,
    blob: BlobRow,
}

impl Backend for PgFeatureBlob {
    type Config = PgFeatureBlobConfig;
    type Response = Vec<tokio_postgres::Row>;

    async fn init(config: &DbConfig<Self::Config>, schema: Arc<Schema>) -> Self {
        let pg = PgContainer::start(
            &config.backend.image,
            config.backend.cgroup_memory_mb,
            &config.backend.shared_buffers,
            &config.backend.work_mem,
            &config.backend.effective_cache_size,
        )
        .await;
        pg.client
            .execute(
                "CREATE TABLE bench (key TEXT PRIMARY KEY, value BYTEA NOT NULL)",
                &[],
            )
            .await
            .expect("failed to create table");
        let read_stmt = pg
            .client
            .prepare_typed(
                "SELECT key, value FROM bench WHERE key = ANY($1)",
                &[Type::TEXT_ARRAY],
            )
            .await
            .expect("failed to prepare read statement");
        PgFeatureBlob {
            pg,
            read_stmt: Arc::new(read_stmt),
            key: StringKey::new(&schema),
            blob: BlobRow::new(&schema),
        }
    }

    async fn write_batch(&self, batch: &RowBatch) {
        let mut blob = Vec::new();
        let sink = self
            .pg
            .client
            .copy_in("COPY bench (key, value) FROM STDIN BINARY")
            .await
            .unwrap();
        let writer = BinaryCopyInWriter::new(sink, &[Type::TEXT, Type::BYTEA]);
        tokio::pin!(writer);

        for row in &batch.rows {
            blob.clear();
            self.blob.encode(row, &mut blob);
            let key = self.key.encode(&row.key);
            let fields: [&(dyn ToSql + Sync); 2] = [&key, &blob];
            writer.as_mut().write(&fields).await.unwrap();
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

    async fn read(&self, request: &Request) -> Self::Response {
        let keys: Vec<Cow<str>> = request.keys.iter().map(|k| self.key.encode(k)).collect();
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
    use crate::config::DbConfig;
    use crate::testing::test_backend_roundtrip;

    #[tokio::test]
    async fn roundtrip() {
        let config = DbConfig {
            write_batch_size: 50,
            measurement_time_secs: 1,
            warmup_time_secs: 1,
            sample_size: 1,
            backend: PgFeatureBlobConfig {
                image: "postgres:18.6".to_string(),
                cgroup_memory_mb: None,
                shared_buffers: default_shared_buffers(),
                work_mem: default_work_mem(),
                effective_cache_size: default_effective_cache_size(),
            },
        };
        test_backend_roundtrip::<PgFeatureBlob>(config).await;
    }
}
