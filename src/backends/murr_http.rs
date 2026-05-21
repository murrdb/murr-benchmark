use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::json;
use testcontainers::core::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use murr::conf::{BackendConfig as StorageBackend, StorageConfig};

use crate::backend::{Backend, Batch};
use crate::config::{BackendConfig, BenchConfig};
use crate::testdata;

const MURR_PORT: u16 = 8080;
const CONTAINER_DATA_DIR: &str = "/tmp/murr-bench";
const CONTAINER_CONFIG_PATH: &str = "/etc/murr/config.yaml";

#[derive(Debug, Clone, Deserialize)]
pub struct MurrHttpConfig {
    pub image: String,
    #[serde(default)]
    pub cgroup_memory_mb: Option<i64>,
    #[serde(default, flatten)]
    pub storage: StorageBackend,
}

#[derive(Serialize)]
struct MurrServerYaml {
    storage: StorageConfig,
}

impl BackendConfig for MurrHttpConfig {}

#[derive(Clone)]
pub struct MurrHttp {
    client: reqwest::Client,
    base_url: String,
    _container: Arc<ContainerAsync<GenericImage>>,
}

impl MurrHttp {
    fn parse_image(image: &str) -> (String, String) {
        match image.rsplit_once(':') {
            Some((name, tag)) => (name.to_string(), tag.to_string()),
            None => (image.to_string(), "latest".to_string()),
        }
    }

    fn build_schema_body(num_cols: usize) -> serde_json::Value {
        let mut columns = serde_json::Map::new();
        columns.insert(
            "key".to_string(),
            json!({"dtype": "utf8", "nullable": false}),
        );
        for name in testdata::column_names(num_cols) {
            columns.insert(name, json!({"dtype": "float32", "nullable": false}));
        }
        json!({
            "key": "key",
            "columns": columns,
        })
    }
}

impl Backend for MurrHttp {
    type Config = MurrHttpConfig;
    type Response = Vec<u8>;

    async fn init(config: &BenchConfig<Self::Config>) -> Self {
        let (image_name, image_tag) = Self::parse_image(&config.backend.image);

        let server_yaml = serde_yaml_ng::to_string(&MurrServerYaml {
            storage: StorageConfig {
                path: PathBuf::from(CONTAINER_DATA_DIR),
                backend: config.backend.storage.clone(),
            },
        })
        .expect("failed to serialize murr server config");
        let cgroup_memory_mb = config.backend.cgroup_memory_mb;
        let container = GenericImage::new(image_name, image_tag)
            .with_exposed_port(MURR_PORT.into())
            .with_wait_for(testcontainers::core::WaitFor::message_on_stderr(
                "Starting murr",
            ))
            .with_copy_to(CONTAINER_CONFIG_PATH, server_yaml.into_bytes())
            .with_cmd(["--config", CONTAINER_CONFIG_PATH])
            .with_host_config_modifier(move |hc| {
                hc.memory = cgroup_memory_mb.map(|mb| mb * 1024 * 1024)
            })
            .start()
            .await
            .expect("failed to start murrdb container");

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(MURR_PORT).await.unwrap();
        let base_url = format!("http://{host}:{port}");

        let client = reqwest::Client::new();

        // Wait for health endpoint
        loop {
            match client.get(format!("{base_url}/health")).send().await {
                Ok(resp) if resp.status().is_success() => break,
                _ => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }

        // Create table
        let schema_body = Self::build_schema_body(config.select_cols);
        let resp = client
            .put(format!("{base_url}/api/v1/table/bench"))
            .json(&schema_body)
            .send()
            .await
            .expect("failed to create table");
        assert!(
            resp.status().is_success(),
            "create table failed: {}",
            resp.status()
        );

        MurrHttp {
            client,
            base_url,
            _container: Arc::new(container),
        }
    }

    async fn write_batch(&self, batch: &Batch) {
        let mut buf = Vec::new();
        {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut buf, batch.inner.schema().as_ref())
                    .expect("failed to create IPC writer");
            writer.write(&batch.inner).expect("failed to write batch");
            writer.finish().expect("failed to finish IPC stream");
        }

        let resp = self
            .client
            .put(format!("{}/api/v1/table/bench/write", self.base_url))
            .header("content-type", "application/vnd.apache.arrow.stream")
            .body(buf)
            .send()
            .await
            .expect("failed to write batch");
        assert!(
            resp.status().is_success(),
            "write batch failed: {}",
            resp.status()
        );
    }

    async fn read(&self, keys: &[String], columns: &[String]) -> Self::Response {
        let body = json!({
            "keys": keys,
            "columns": columns,
        });
        let resp = self
            .client
            .post(format!("{}/api/v1/table/bench/fetch", self.base_url))
            .header("accept", "application/vnd.apache.arrow.stream")
            .json(&body)
            .send()
            .await
            .unwrap();
        resp.bytes().await.unwrap().to_vec()
    }

    async fn memory_usage(&self) -> crate::stats::mem::MemoryUsage {
        crate::stats::mem::MemoryUsage::for_container(self._container.id()).await
    }

    async fn disk_usage(&self) -> crate::stats::disk::DiskUsage {
        crate::stats::disk::DiskUsage::for_container(self._container.id()).await
    }

    async fn network_usage(&self) -> crate::backend::NetworkUsage {
        crate::stats::net::NetworkUsage::for_container(self._container.id()).await
    }

    async fn cleanup(self) {
        drop(self._container);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BenchConfig;
    use crate::testing::test_backend_roundtrip;
    use murr::io::store::rocksdb::block::BlockConfig;
    use murr::io::store::rocksdb::plain::PlainConfig;

    fn base_config(storage: StorageBackend) -> BenchConfig<MurrHttpConfig> {
        BenchConfig {
            total_rows: 100,
            select_rows: 10,
            select_cols: 2,
            write_batch_size: 50,
            measurement_time_secs: 1,
            warmup_time_secs: 1,
            sample_size: 1,
            backend: MurrHttpConfig {
                image: "ghcr.io/murrdb/murr:latest".to_string(),
                cgroup_memory_mb: None,
                storage,
            },
        }
    }

    #[tokio::test]
    async fn roundtrip_mmap() {
        let config = base_config(StorageBackend::Mmap(PlainConfig::default()));
        test_backend_roundtrip::<MurrHttp>(config).await;
    }

    #[tokio::test]
    async fn roundtrip_block() {
        let config = base_config(StorageBackend::Block(BlockConfig::default()));
        test_backend_roundtrip::<MurrHttp>(config).await;
    }
}
