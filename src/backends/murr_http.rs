use std::sync::Arc;

use serde::Deserialize;
use serde_json::json;
use testcontainers::GenericImage;
use testcontainers::core::ContainerAsync;
use testcontainers::runners::AsyncRunner;

use crate::backend::{Backend, Batch};
use crate::config::{BackendConfig, BenchConfig};
use crate::testdata;

const MURR_PORT: u16 = 8080;

#[derive(Debug, Clone, Deserialize)]
pub struct MurrHttpConfig {
    pub image: String,
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

        let container = GenericImage::new(image_name, image_tag)
            .with_exposed_port(MURR_PORT.into())
            .with_wait_for(testcontainers::core::WaitFor::message_on_stderr("listening"))
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
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(
                &mut buf,
                batch.inner.schema().as_ref(),
            )
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

    async fn cleanup(self) {
        drop(self._container);
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
            backend: MurrHttpConfig {
                image: "ghcr.io/murrdb/murr:latest".to_string(),
            },
        };
        test_backend_roundtrip::<MurrHttp>(config).await;
    }
}
