use std::path::Path;

use indexmap::IndexMap;
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

/// A YAML benchmark suite: shared settings plus one or more named backend variants.
/// Each variant produces a separate `BenchConfig<B>` for an independent benchmark run.
#[derive(Debug, Clone, Deserialize)]
#[serde(bound = "B: BackendConfig")]
pub struct BenchSuite<B: BackendConfig> {
    pub total_rows: usize,
    pub select_rows: usize,
    pub select_cols: usize,
    pub write_batch_size: usize,
    pub measurement_time_secs: u64,
    pub warmup_time_secs: u64,
    pub sample_size: usize,
    pub backend: IndexMap<String, B>,
}

impl<B: BackendConfig> BenchSuite<B> {
    pub fn from_file(path: impl AsRef<Path>) -> Self {
        let contents = std::fs::read_to_string(path).expect("failed to read config file");
        serde_yaml_ng::from_str(&contents).expect("failed to parse config YAML")
    }

    pub fn variants(&self) -> impl Iterator<Item = (String, BenchConfig<B>)> + '_ {
        self.backend.iter().map(move |(name, b)| {
            (
                name.clone(),
                BenchConfig {
                    total_rows: self.total_rows,
                    select_rows: self.select_rows,
                    select_cols: self.select_cols,
                    write_batch_size: self.write_batch_size,
                    measurement_time_secs: self.measurement_time_secs,
                    warmup_time_secs: self.warmup_time_secs,
                    sample_size: self.sample_size,
                    backend: b.clone(),
                },
            )
        })
    }
}
