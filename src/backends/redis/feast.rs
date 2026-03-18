use std::collections::HashMap;

use serde::Deserialize;

use crate::backend::{Backend, Batch};
use crate::config::{BackendConfig, BenchConfig};

use super::RedisContainer;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReadMode {
    Hgetall,
    Hmget,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisFeastConfig {
    pub image: String,
    pub read_mode: ReadMode,
}

impl BackendConfig for RedisFeastConfig {}

#[derive(Clone)]
pub struct RedisFeast {
    redis: RedisContainer,
    read_mode: ReadMode,
}

impl Backend for RedisFeast {
    type Config = RedisFeastConfig;
    type Response = Vec<HashMap<String, Vec<u8>>>;

    async fn init(config: &BenchConfig<Self::Config>) -> Self {
        let redis = RedisContainer::start(&config.backend.image).await;
        RedisFeast {
            redis,
            read_mode: config.backend.read_mode.clone(),
        }
    }

    async fn write_batch(&self, batch: &Batch) {
        let mut con = self.redis.con.clone();
        let value_cols = batch.value_columns();

        let mut pipe = redis::pipe();
        for (row, key) in batch.keys.iter().enumerate() {
            let fields: Vec<(&str, Vec<u8>)> = batch
                .columns
                .iter()
                .zip(&value_cols)
                .map(|(name, col)| (name.as_str(), col.value(row).to_le_bytes().to_vec()))
                .collect();
            pipe.hset_multiple(key.as_str(), &fields).ignore();
        }
        pipe.query_async::<()>(&mut con).await.unwrap();
    }

    async fn read(&self, keys: &[String], columns: &[String]) -> Self::Response {
        let mut con = self.redis.con.clone();
        match self.read_mode {
            ReadMode::Hgetall => {
                let mut pipe = redis::pipe();
                for key in keys {
                    pipe.hgetall(key);
                }
                pipe.query_async(&mut con).await.unwrap()
            }
            ReadMode::Hmget => {
                let mut pipe = redis::pipe();
                let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                for key in keys {
                    pipe.cmd("HMGET").arg(key).arg(&col_refs);
                }
                let results: Vec<Vec<Option<Vec<u8>>>> =
                    pipe.query_async(&mut con).await.unwrap();
                results
                    .into_iter()
                    .map(|vals| {
                        columns
                            .iter()
                            .zip(vals)
                            .filter_map(|(col, val)| val.map(|v| (col.clone(), v)))
                            .collect()
                    })
                    .collect()
            }
        }
    }

    async fn memory_usage(&self) -> crate::backend::MemoryUsage {
        crate::stats::mem::MemoryUsage::for_container(self.redis._container.id()).await
    }

    async fn disk_usage(&self) -> crate::backend::DiskUsage {
        crate::stats::disk::DiskUsage::for_container(self.redis._container.id()).await
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
    async fn roundtrip_hgetall() {
        let config = BenchConfig {
            total_rows: 100,
            select_rows: 10,
            select_cols: 2,
            write_batch_size: 50,
            measurement_time_secs: 1,
            warmup_time_secs: 1,
            sample_size: 1,
            backend: RedisFeastConfig {
                image: "redis:latest".to_string(),
                read_mode: ReadMode::Hgetall,
            },
        };
        test_backend_roundtrip::<RedisFeast>(config).await;
    }

    #[tokio::test]
    async fn roundtrip_hmget() {
        let config = BenchConfig {
            total_rows: 100,
            select_rows: 10,
            select_cols: 2,
            write_batch_size: 50,
            measurement_time_secs: 1,
            warmup_time_secs: 1,
            sample_size: 1,
            backend: RedisFeastConfig {
                image: "redis:latest".to_string(),
                read_mode: ReadMode::Hmget,
            },
        };
        test_backend_roundtrip::<RedisFeast>(config).await;
    }
}
