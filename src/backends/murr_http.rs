use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use testcontainers::core::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use murr::api::fetch::COLUMNS_METADATA;
use murr::conf::{BackendConfig as StorageBackend, StorageConfig};
use murr::core::TableSchema;

use crate::backend::Backend;
use crate::codec::arrow::{ArrowBatch, ArrowKeys};
use crate::codec::{BatchEncoder, KeySetEncoder};
use crate::config::{BackendConfig, DbConfig};
use crate::workload::{Request, RowBatch, Schema};

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
    rows: ArrowBatch,
    keys: ArrowKeys,
}

impl MurrHttp {
    fn parse_image(image: &str) -> (String, String) {
        match image.rsplit_once(':') {
            Some((name, tag)) => (name.to_string(), tag.to_string()),
            None => (image.to_string(), "latest".to_string()),
        }
    }
}

impl Backend for MurrHttp {
    type Config = MurrHttpConfig;
    type Response = bytes::Bytes;

    async fn init(config: &DbConfig<Self::Config>, schema: Arc<Schema>) -> Self {
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
                "HTTP listen",
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
        let resp = client
            .put(format!("{base_url}/api/v1/table/bench"))
            .json(&TableSchema::from(schema.as_ref()))
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
            rows: ArrowBatch::new(schema.clone()),
            keys: ArrowKeys::new(schema),
        }
    }

    async fn write_batch(&self, batch: &RowBatch) {
        let record_batch = self.rows.encode(batch);
        let mut buf = Vec::new();
        {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut buf, record_batch.schema().as_ref())
                    .expect("failed to create IPC writer");
            writer.write(&record_batch).expect("failed to write batch");
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

    async fn read(&self, request: &Request) -> Self::Response {
        // An IPC fetch request is the key columns as a batch, with the columns
        // to return listed in the schema metadata.
        let keys = self.keys.encode(&request.keys);
        let columns = serde_json::to_string(&request.columns).expect("failed to encode columns");
        let schema = keys
            .schema()
            .as_ref()
            .clone()
            .with_metadata(HashMap::from([(COLUMNS_METADATA.to_string(), columns)]));
        let keys = keys
            .with_schema(Arc::new(schema))
            .expect("failed to attach columns metadata");
        let mut buf = Vec::new();
        {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut buf, keys.schema().as_ref())
                    .expect("failed to create IPC writer");
            writer.write(&keys).expect("failed to write keys");
            writer.finish().expect("failed to finish IPC stream");
        }

        let resp = self
            .client
            .post(format!("{}/api/v1/table/bench/fetch", self.base_url))
            .header("content-type", "application/vnd.apache.arrow.stream")
            .header("accept", "application/vnd.apache.arrow.stream")
            .body(buf)
            .send()
            .await
            .unwrap();
        resp.bytes().await.unwrap()
    }

    async fn flush(&self) {
        let resp = self
            .client
            .post(format!("{}/api/v1/table/bench/compact", self.base_url))
            .send()
            .await
            .expect("failed to compact table");
        assert!(
            resp.status().is_success(),
            "compact failed: {}",
            resp.status()
        );
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
    use crate::config::DbConfig;
    use crate::testing::test_backend_roundtrip;
    use murr::io::store::rocksdb::block::BlockConfig;
    use murr::io::store::rocksdb::plain::PlainConfig;

    fn base_config(storage: StorageBackend) -> DbConfig<MurrHttpConfig> {
        DbConfig {
            write_batch_size: 50,
            measurement_time_secs: 1,
            warmup_time_secs: 1,
            sample_size: 1,
            backend: MurrHttpConfig {
                image: "ghcr.io/murrdb/murr:0.3.0".to_string(),
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
