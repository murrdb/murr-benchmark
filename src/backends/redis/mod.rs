pub mod feast;
pub mod featureblob;

use std::sync::Arc;

use testcontainers::core::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{Redis, REDIS_PORT};

/// Shared Redis container + multiplexed connection wrapper.
#[derive(Clone)]
pub struct RedisContainer {
    pub con: redis::aio::MultiplexedConnection,
    _container: Arc<ContainerAsync<Redis>>,
}

impl RedisContainer {
    pub async fn start() -> Self {
        let container = Redis::default().start().await.unwrap();
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
