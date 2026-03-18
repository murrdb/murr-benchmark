use std::fmt;

use bollard::query_parameters::StatsOptionsBuilder;
use bollard::Docker;
use futures_util::StreamExt;

pub struct MemoryUsage {
    pub rss_bytes: u64,
    pub shared_bytes: u64,
    pub virt_bytes: u64,
}

impl MemoryUsage {
    /// Measures memory usage of a Docker container via the Docker stats API.
    pub async fn for_container(container_id: &str) -> MemoryUsage {
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

        let mem = stats
            .memory_stats
            .as_ref()
            .expect("no memory stats in container stats response");

        let rss_bytes = mem
            .stats
            .as_ref()
            .and_then(|s| s.get("rss").or_else(|| s.get("anon")))
            .copied()
            .unwrap_or_else(|| mem.usage.unwrap_or(0));

        let shared_bytes = mem
            .stats
            .as_ref()
            .and_then(|s| s.get("cache").or_else(|| s.get("file")))
            .copied()
            .unwrap_or(0);

        let virt_bytes = mem.usage.unwrap_or(0);

        MemoryUsage {
            rss_bytes,
            shared_bytes,
            virt_bytes,
        }
    }

    /// Measures memory usage of the current process via /proc/self/statm.
    pub fn for_process() -> MemoryUsage {
        let statm =
            std::fs::read_to_string("/proc/self/statm").expect("failed to read /proc/self/statm");
        let parts: Vec<u64> = statm
            .split_whitespace()
            .take(3)
            .map(|s| s.parse().expect("failed to parse /proc/self/statm field"))
            .collect();

        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as u64 };

        MemoryUsage {
            rss_bytes: parts[1] * page_size,
            shared_bytes: parts[2] * page_size,
            virt_bytes: parts[0] * page_size,
        }
    }

    pub fn diff(&self, other: &MemoryUsage) -> MemoryUsage {
        MemoryUsage {
            rss_bytes: other.rss_bytes.saturating_sub(self.rss_bytes),
            shared_bytes: other.shared_bytes.saturating_sub(self.shared_bytes),
            virt_bytes: other.virt_bytes.saturating_sub(self.virt_bytes),
        }
    }
}

impl fmt::Debug for MemoryUsage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RSS={:.2} MiB, SHR={:.2} MiB, VIRT={:.2} MiB",
            self.rss_bytes as f64 / 1048576.0,
            self.shared_bytes as f64 / 1048576.0,
            self.virt_bytes as f64 / 1048576.0,
        )
    }
}
