pub mod feast;
pub mod featureblob;

use std::sync::Arc;

use testcontainers::GenericImage;
use testcontainers::ImageExt;
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
    pub async fn start(
        image: &str,
        cgroup_memory_mb: Option<i64>,
        command: Vec<String>,
        wait_log: &str,
    ) -> Self {
        let (name, tag) = match image.rsplit_once(':') {
            Some((n, t)) => (n, t),
            None => (image, "latest"),
        };

        let container = GenericImage::new(name, tag)
            .with_exposed_port(REDIS_PORT.into())
            .with_wait_for(testcontainers::core::WaitFor::message_on_either_std(wait_log))
            .with_cmd(command)
            .with_host_config_modifier(move |hc| {
                hc.memory = cgroup_memory_mb.map(|mb| mb * 1024 * 1024)
            })
            .start()
            .await
            .expect("failed to start Redis container");

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(REDIS_PORT).await.unwrap();
        let client = redis::Client::open(format!("redis://{host}:{port}")).unwrap();
        // redis-rs 1.x defaults to a 500ms per-response timeout, which is far
        // shorter than the time a 100k-command pipeline takes to round-trip
        // through the docker proxy. Disable it for benchmark workloads.
        let aio_config = redis::AsyncConnectionConfig::new().set_response_timeout(None);
        let con = client
            .get_multiplexed_async_connection_with_config(&aio_config)
            .await
            .unwrap();
        Self {
            con,
            _container: Arc::new(container),
        }
    }
}
