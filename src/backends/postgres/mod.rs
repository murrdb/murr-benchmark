pub mod feast;
pub mod featureblob;

use std::sync::Arc;

use testcontainers::GenericImage;
use testcontainers::ImageExt;
use testcontainers::core::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use tokio_postgres::NoTls;

use crate::stats::disk::DiskUsage;

const PG_PORT: u16 = 5432;

pub fn default_shared_buffers() -> String {
    "128MB".to_string()
}
pub fn default_work_mem() -> String {
    "4MB".to_string()
}
pub fn default_effective_cache_size() -> String {
    "4GB".to_string()
}

/// Shared Postgres container + client wrapper.
#[derive(Clone)]
pub struct PgContainer {
    pub client: Arc<tokio_postgres::Client>,
    _container: Arc<ContainerAsync<GenericImage>>,
}

impl PgContainer {
    pub async fn start(
        image: &str,
        cgroup_memory_mb: Option<i64>,
        shared_buffers: &str,
        work_mem: &str,
        effective_cache_size: &str,
    ) -> Self {
        let (name, tag) = match image.rsplit_once(':') {
            Some((n, t)) => (n, t),
            None => (image, "latest"),
        };

        let container = GenericImage::new(name, tag)
            .with_exposed_port(PG_PORT.into())
            .with_wait_for(testcontainers::core::WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_PASSWORD", "bench")
            .with_env_var("POSTGRES_DB", "bench")
            .with_cmd([
                "postgres".to_string(),
                "-c".to_string(),
                format!("shared_buffers={shared_buffers}"),
                "-c".to_string(),
                format!("work_mem={work_mem}"),
                "-c".to_string(),
                format!("effective_cache_size={effective_cache_size}"),
                "-c".to_string(),
                "effective_io_concurrency=200".to_string(),
                "-c".to_string(),
                "max_wal_size=4GB".to_string(),
            ])
            .with_host_config_modifier(move |hc| {
                hc.memory = cgroup_memory_mb.map(|mb| mb * 1024 * 1024)
            })
            .start()
            .await
            .expect("failed to start Postgres container");

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(PG_PORT).await.unwrap();

        let (client, connection) = tokio_postgres::connect(
            &format!("host={host} port={port} user=postgres password=bench dbname=bench"),
            NoTls,
        )
        .await
        .expect("failed to connect to Postgres");

        // Spawn the connection handler in the background.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("postgres connection error: {e}");
            }
        });

        Self {
            client: Arc::new(client),
            _container: Arc::new(container),
        }
    }

    pub async fn disk_usage(&self) -> DiskUsage {
        let row = self
            .client
            .query_one("SELECT pg_database_size('bench')", &[])
            .await
            .expect("failed to query pg_database_size");
        let used_bytes: i64 = row.get(0);
        DiskUsage {
            used_bytes: used_bytes as u64,
        }
    }
}
