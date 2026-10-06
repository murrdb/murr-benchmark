use std::sync::Arc;

use crate::config::{BackendConfig, DbConfig};
pub use crate::stats::disk::DiskUsage;
pub use crate::stats::mem::MemoryUsage;
pub use crate::stats::net::NetworkUsage;
use crate::workload::{Request, RowBatch, Schema};

/// A benchmarkable storage backend.
///
/// Not object-safe — used via generics (`Bench::run::<B>`).
/// `Clone` is required because Criterion's async closure clones the handle per iteration.
pub trait Backend: Sized + Send + Sync + Clone {
    type Config: BackendConfig;
    type Response: Send + 'static;

    /// Start containers, create the table for `schema`, prepare connections.
    fn init(
        config: &DbConfig<Self::Config>,
        schema: Arc<Schema>,
    ) -> impl Future<Output = Self> + Send;

    /// Write a single batch of rows (already sized to write_batch_size).
    fn write_batch(&self, batch: &RowBatch) -> impl Future<Output = ()> + Send;

    /// Execute a single read — the hot path measured by Criterion.
    fn read(&self, request: &Request) -> impl Future<Output = Self::Response> + Send;

    /// Report current memory usage (RSS, shared, virtual).
    fn memory_usage(&self) -> impl Future<Output = MemoryUsage> + Send;

    /// Report current disk usage.
    fn disk_usage(&self) -> impl Future<Output = DiskUsage> + Send;

    /// Report cumulative network bytes in/out.
    /// Default returns zero — only meaningful for Docker-based backends.
    fn network_usage(&self) -> impl Future<Output = NetworkUsage> + Send {
        async {
            NetworkUsage {
                rx_bytes: 0,
                tx_bytes: 0,
            }
        }
    }

    /// Flush all buffered writes to durable storage.
    /// Called after all write_batch calls, before disk_usage is measured.
    fn flush(&self) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Stop containers, close connections.
    fn cleanup(self) -> impl Future<Output = ()> + Send;
}
