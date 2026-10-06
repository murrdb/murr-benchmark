/// jemalloc for every bench, test and example that links this crate, matching the murr
/// server binary and RocksDB's internal allocator (`rocksdb/jemalloc` feature).
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub mod backend;
pub mod backends;
pub mod bench;
pub mod codec;
pub mod config;
pub mod report;
pub mod stats;
pub mod workload;
pub mod testing;
