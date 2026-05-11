use std::fmt;

use bollard::query_parameters::StatsOptionsBuilder;
use bollard::Docker;
use futures_util::StreamExt;

pub struct NetworkUsage {
    pub rx_bytes: u64,
    pub tx_bytes: u64,
}

impl NetworkUsage {
    /// Measures network usage of a Docker container via the Docker stats API.
    /// Sums rx/tx bytes across all interfaces.
    pub async fn for_container(container_id: &str) -> NetworkUsage {
        let docker = Docker::connect_with_local_defaults().expect("failed to connect to Docker");

        let options = StatsOptionsBuilder::default()
            .stream(false)
            .one_shot(true)
            .build();

        let stats = docker
            .stats(container_id, Some(options))
            .next()
            .await
            .expect("no stats returned")
            .expect("failed to get container stats");

        let (rx_bytes, tx_bytes) = stats
            .networks
            .as_ref()
            .map(|nets| {
                nets.values().fold((0u64, 0u64), |(rx, tx), n| {
                    (
                        rx + n.rx_bytes.unwrap_or(0),
                        tx + n.tx_bytes.unwrap_or(0),
                    )
                })
            })
            .unwrap_or((0, 0));

        NetworkUsage { rx_bytes, tx_bytes }
    }

    pub fn diff(&self, other: &NetworkUsage) -> NetworkUsage {
        NetworkUsage {
            rx_bytes: other.rx_bytes.saturating_sub(self.rx_bytes),
            tx_bytes: other.tx_bytes.saturating_sub(self.tx_bytes),
        }
    }
}

impl fmt::Debug for NetworkUsage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RX={:.2} MiB, TX={:.2} MiB",
            self.rx_bytes as f64 / 1048576.0,
            self.tx_bytes as f64 / 1048576.0,
        )
    }
}
