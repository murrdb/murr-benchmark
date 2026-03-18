use redis::AsyncCommands;
use serde::Deserialize;

use crate::backend::{Backend, Batch};
use crate::config::{BackendConfig, BenchConfig};

use super::RedisContainer;

#[derive(Debug, Clone, Deserialize)]
pub struct RedisFeatureBlobConfig {
    pub image: String,
}

impl BackendConfig for RedisFeatureBlobConfig {}

#[derive(Clone)]
pub struct RedisFeatureBlob {
    redis: RedisContainer,
}

impl Backend for RedisFeatureBlob {
    type Config = RedisFeatureBlobConfig;
    type Response = Vec<Option<Vec<u8>>>;

    async fn init(_config: &BenchConfig<Self::Config>) -> Self {
        let redis = RedisContainer::start().await;
        RedisFeatureBlob { redis }
    }

    async fn write_batch(&self, batch: &Batch) {
        let mut con = self.redis.con.clone();
        let value_cols = batch.value_columns();

        let mut pipe = redis::pipe();
        for (row, key) in batch.keys.iter().enumerate() {
            let mut blob = Vec::with_capacity(value_cols.len() * 4);
            for col in &value_cols {
                blob.extend_from_slice(&col.value(row).to_le_bytes());
            }
            pipe.set(key.as_str(), blob).ignore();
        }
        pipe.query_async::<()>(&mut con).await.unwrap();
    }

    async fn read(&self, keys: &[String], _columns: &[String]) -> Self::Response {
        let mut con = self.redis.con.clone();
        con.mget(keys).await.unwrap()
    }

    async fn cleanup(self) {
        drop(self.redis);
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
            backend: RedisFeatureBlobConfig {
                image: "redis:latest".to_string(),
            },
        };
        test_backend_roundtrip::<RedisFeatureBlob>(config).await;
    }
}
