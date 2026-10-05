use std::sync::Arc;

use redis::AsyncCommands;
use serde::Deserialize;

use crate::backend::Backend;
use crate::codec::blob::BlobRow;
use crate::codec::key::StringKey;
use crate::codec::{KeyEncoder, RowEncoder};
use crate::config::{BackendConfig, DbConfig};
use crate::workload::{Request, RowBatch, Schema};

use super::RedisContainer;

#[derive(Debug, Clone, Deserialize)]
pub struct RedisFeatureBlobConfig {
    pub image: String,
    pub command: Vec<String>,
    pub wait_log: String,
    #[serde(default)]
    pub cgroup_memory_mb: Option<i64>,
}

impl BackendConfig for RedisFeatureBlobConfig {}

#[derive(Clone)]
pub struct RedisFeatureBlob {
    redis: RedisContainer,
    key: StringKey,
    blob: BlobRow,
}

impl Backend for RedisFeatureBlob {
    type Config = RedisFeatureBlobConfig;
    type Response = Vec<Option<Vec<u8>>>;

    async fn init(config: &DbConfig<Self::Config>, schema: Arc<Schema>) -> Self {
        let redis = RedisContainer::start(
            &config.backend.image,
            config.backend.cgroup_memory_mb,
            config.backend.command.clone(),
            &config.backend.wait_log,
        )
        .await;
        RedisFeatureBlob {
            redis,
            key: StringKey::new(&schema),
            blob: BlobRow::new(&schema),
        }
    }

    async fn write_batch(&self, batch: &RowBatch) {
        let mut con = self.redis.con.clone();

        let mut items: Vec<(&str, Vec<u8>)> = Vec::with_capacity(batch.rows.len());
        for row in &batch.rows {
            let mut blob = Vec::new();
            self.blob.encode(row, &mut blob);
            items.push((self.key.encode(&row.key), blob));
        }
        let _: () = con.mset(&items).await.unwrap();
    }

    async fn read(&self, request: &Request) -> Self::Response {
        let mut con = self.redis.con.clone();
        let keys: Vec<&str> = request.keys.iter().map(|k| self.key.encode(k)).collect();
        con.mget(keys).await.unwrap()
    }

    async fn memory_usage(&self) -> crate::backend::MemoryUsage {
        crate::stats::mem::MemoryUsage::for_container(self.redis._container.id()).await
    }

    async fn disk_usage(&self) -> crate::backend::DiskUsage {
        crate::stats::disk::DiskUsage::for_container(self.redis._container.id()).await
    }

    async fn network_usage(&self) -> crate::backend::NetworkUsage {
        crate::stats::net::NetworkUsage::for_container(self.redis._container.id()).await
    }

    async fn cleanup(self) {
        drop(self.redis);
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
            backend: RedisFeatureBlobConfig {
                image: "redis:8.10.2".to_string(),
                command: ["redis-server", "--save", "", "--appendonly", "no"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                wait_log: "Ready to accept connections".to_string(),
                cgroup_memory_mb: None,
            },
        };
        test_backend_roundtrip::<RedisFeatureBlob>(config).await;
    }
}
