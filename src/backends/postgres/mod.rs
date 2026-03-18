pub mod feast;
pub mod featureblob;

use std::sync::Arc;

use testcontainers::GenericImage;
use testcontainers::ImageExt;
use testcontainers::core::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use tokio_postgres::NoTls;

const PG_PORT: u16 = 5432;

/// Shared Postgres container + client wrapper.
#[derive(Clone)]
pub struct PgContainer {
    pub client: Arc<tokio_postgres::Client>,
    _container: Arc<ContainerAsync<GenericImage>>,
}

impl PgContainer {
    pub async fn start(image: &str) -> Self {
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
}
