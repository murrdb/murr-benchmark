pub mod feast;
pub mod featureblob;

use std::sync::Arc;

use testcontainers::GenericImage;
use testcontainers::core::ContainerAsync;
use testcontainers::runners::AsyncRunner;

const REDIS_PORT: u16 = 6379;

/// Shared Redis container + multiplexed connection wrapper.
#[derive(Clone)]
pub struct RedisContainer {
    pub con: redis::aio::MultiplexedConnection,
    _container: Arc<ContainerAsync<GenericImage>>,
}

impl RedisContainer {
    pub async fn start(image: &str) -> Self {
        let (name, tag) = match image.rsplit_once(':') {
            Some((n, t)) => (n, t),
            None => (image, "latest"),
        };

        let container = GenericImage::new(name, tag)
            .with_exposed_port(REDIS_PORT.into())
            .with_wait_for(testcontainers::core::WaitFor::message_on_stdout("Ready to accept connections"))
            .start()
            .await
            .expect("failed to start Redis container");

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(REDIS_PORT).await.unwrap();
        let client = redis::Client::open(format!("redis://{host}:{port}")).unwrap();
        let con = client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        Self {
            con,
            _container: Arc::new(container),
        }
    }
}
