use std::path::Path;

use serde::Deserialize;
use serde::de::DeserializeOwned;

/// Per-backend configuration. Each backend defines its own config struct.
pub trait BackendConfig: DeserializeOwned + Clone + std::fmt::Debug {}

/// Shared benchmark settings, common to all backends.
#[derive(Debug, Clone, Deserialize)]
#[serde(bound = "B: BackendConfig")]
pub struct BenchConfig<B: BackendConfig> {
    pub total_rows: usize,
    pub select_rows: usize,
    pub select_cols: usize,
    pub write_batch_size: usize,
    pub measurement_time_secs: u64,
    pub warmup_time_secs: u64,
    pub sample_size: usize,
    pub backend: B,
}

impl<B: BackendConfig> BenchConfig<B> {
    pub fn from_file(path: impl AsRef<Path>) -> Self {
        let contents = std::fs::read_to_string(path).expect("failed to read config file");
        serde_yaml::from_str(&contents).expect("failed to parse config YAML")
    }
}
