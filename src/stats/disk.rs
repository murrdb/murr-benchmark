use std::fmt;
use std::path::Path;

use bollard::Docker;
use bollard::query_parameters::InspectContainerOptions;

pub struct DiskUsage {
    pub used_bytes: u64,
}

impl DiskUsage {
    /// Measures disk usage of a Docker container's writable layer via the Docker inspect API.
    pub async fn for_container(container_id: &str) -> DiskUsage {
        let docker = Docker::connect_with_local_defaults().expect("failed to connect to Docker");

        let inspect = docker
            .inspect_container(container_id, Some(InspectContainerOptions { size: true }))
            .await
            .expect("failed to inspect container");

        let used_bytes = inspect.size_rw.unwrap_or(0) as u64;

        DiskUsage { used_bytes }
    }

    /// Measures disk usage by recursively walking a directory and summing file sizes.
    pub fn for_path(path: &Path) -> DiskUsage {
        let mut total: u64 = 0;
        let mut stack = vec![path.to_path_buf()];
        while let Some(dir) = stack.pop() {
            if let Ok(entries) = std::fs::read_dir(&dir) {
                for entry in entries.flatten() {
                    let meta = match entry.metadata() {
                        Ok(m) => m,
                        Err(_) => continue,
                    };
                    if meta.is_dir() {
                        stack.push(entry.path());
                    } else {
                        total += meta.len();
                    }
                }
            }
        }
        DiskUsage { used_bytes: total }
    }

    pub fn diff(&self, other: &DiskUsage) -> DiskUsage {
        DiskUsage {
            used_bytes: other.used_bytes.saturating_sub(self.used_bytes),
        }
    }
}

impl fmt::Debug for DiskUsage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DISK={:.2} MiB",
            self.used_bytes as f64 / 1048576.0,
        )
    }
}
