use std::sync::Arc;

use serde::Deserialize;

use crate::backend::Backend;
use crate::codec::blob::LittleEndian;
use crate::codec::key::StringKey;
use crate::codec::{KeyEncoder, ValueEncoder};
use crate::config::{BackendConfig, DbConfig};
use crate::workload::{Request, RowBatch, Schema};

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
    pub command: Vec<String>,
    pub wait_log: String,
    #[serde(default)]
    pub cgroup_memory_mb: Option<i64>,
}

impl BackendConfig for RedisFeastConfig {}

#[derive(Clone)]
pub struct RedisFeast {
    redis: RedisContainer,
    read_mode: ReadMode,
    schema: Arc<Schema>,
    key: StringKey,
}

impl Backend for RedisFeast {
    type Config = RedisFeastConfig;
    type Response = Vec<redis::Value>;

    async fn init(config: &DbConfig<Self::Config>, schema: Arc<Schema>) -> Self {
        let redis = RedisContainer::start(
            &config.backend.image,
            config.backend.cgroup_memory_mb,
            config.backend.command.clone(),
            &config.backend.wait_log,
        )
        .await;
        RedisFeast {
            redis,
            read_mode: config.backend.read_mode.clone(),
            key: StringKey::new(&schema),
            schema,
        }
    }

    async fn write_batch(&self, batch: &RowBatch) {
        let mut con = self.redis.con.clone();

        let mut pipe = redis::pipe();
        for row in &batch.rows {
            // Null values are not stored: the hash simply has no such field.
            let fields: Vec<(&str, Vec<u8>)> = self
                .schema
                .values
                .iter()
                .zip(&row.values)
                .filter_map(|(field, value)| {
                    let mut bytes = Vec::new();
                    LittleEndian.encode(value.as_ref()?, &mut bytes);
                    Some((field.name.as_str(), bytes))
                })
                .collect();
            if fields.is_empty() {
                continue;
            }
            pipe.hset_multiple(self.key.encode(&row.key), &fields).ignore();
        }
        pipe.query_async::<()>(&mut con).await.unwrap();
    }

    async fn read(&self, request: &Request) -> Self::Response {
        let mut con = self.redis.con.clone();
        let mut pipe = redis::pipe();
        match self.read_mode {
            ReadMode::Hgetall => {
                for key in &request.keys {
                    pipe.hgetall(self.key.encode(key));
                }
            }
            ReadMode::Hmget => {
                let col_refs: Vec<&str> = request.columns.iter().map(|s| s.as_str()).collect();
                for key in &request.keys {
                    pipe.cmd("HMGET").arg(self.key.encode(key)).arg(&col_refs);
                }
            }
        }
        pipe.query_async(&mut con).await.unwrap()
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

    fn redis_command() -> Vec<String> {
        ["redis-server", "--save", "", "--appendonly", "no"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[tokio::test]
    async fn roundtrip_hgetall() {
        let config = DbConfig {
            write_batch_size: 50,
            measurement_time_secs: 1,
            warmup_time_secs: 1,
            sample_size: 1,
            backend: RedisFeastConfig {
                image: "redis:8.10.2".to_string(),
                read_mode: ReadMode::Hgetall,
                command: redis_command(),
                wait_log: "Ready to accept connections".to_string(),
                cgroup_memory_mb: None,
            },
        };
        test_backend_roundtrip::<RedisFeast>(config).await;
    }

    #[tokio::test]
    async fn roundtrip_hmget() {
        let config = DbConfig {
            write_batch_size: 50,
            measurement_time_secs: 1,
            warmup_time_secs: 1,
            sample_size: 1,
            backend: RedisFeastConfig {
                image: "redis:8.10.2".to_string(),
                read_mode: ReadMode::Hmget,
                command: redis_command(),
                wait_log: "Ready to accept connections".to_string(),
                cgroup_memory_mb: None,
            },
        };
        test_backend_roundtrip::<RedisFeast>(config).await;
    }
}
