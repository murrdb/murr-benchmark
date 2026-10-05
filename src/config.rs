use std::path::Path;

use indexmap::IndexMap;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::workload::Workload;
use crate::workload::file::{FileConfig, FileWorkload};
use crate::workload::synthetic::{SyntheticConfig, SyntheticWorkload};

/// Per-backend configuration. Each backend defines its own config struct.
pub trait BackendConfig: DeserializeOwned + Clone + std::fmt::Debug {}

/// Database-side settings for one backend variant: ingest batching,
/// measurement timings and the backend's own config.
#[derive(Debug, Clone, Deserialize)]
#[serde(bound = "B: BackendConfig")]
pub struct DbConfig<B: BackendConfig> {
    pub write_batch_size: usize,
    pub measurement_time_secs: u64,
    pub warmup_time_secs: u64,
    pub sample_size: usize,
    pub backend: B,
}

/// A YAML file from `configs/db/`: shared settings plus one or more named backend variants.
/// Each variant produces a separate `DbConfig<B>` for an independent benchmark run.
#[derive(Debug, Clone, Deserialize)]
#[serde(bound = "B: BackendConfig")]
pub struct DbSuite<B: BackendConfig> {
    pub write_batch_size: usize,
    pub measurement_time_secs: u64,
    pub warmup_time_secs: u64,
    pub sample_size: usize,
    pub backend: IndexMap<String, B>,
}

impl<B: BackendConfig> DbSuite<B> {
    pub fn from_file(path: impl AsRef<Path>) -> Self {
        let contents = std::fs::read_to_string(path).expect("failed to read config file");
        serde_yaml_ng::from_str(&contents).expect("failed to parse config YAML")
    }

    pub fn variants(&self) -> impl Iterator<Item = (String, DbConfig<B>)> + '_ {
        self.backend.iter().map(move |(name, b)| {
            (
                name.clone(),
                DbConfig {
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

/// A YAML file from `configs/workload/`, selected by its `type` key.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum WorkloadConfig {
    Synthetic(SyntheticConfig),
    File(FileConfig),
}

impl WorkloadConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Self {
        let contents = std::fs::read_to_string(path).expect("failed to read workload file");
        serde_yaml_ng::from_str(&contents).expect("failed to parse workload YAML")
    }

    pub fn build(self) -> Box<dyn Workload> {
        match self {
            WorkloadConfig::Synthetic(config) => Box::new(SyntheticWorkload::new(config)),
            WorkloadConfig::File(config) => Box::new(FileWorkload::new(config)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::DType;

    #[test]
    fn synthetic_workload_files_differ_only_in_row_count() {
        for (name, total_rows) in [("1m", 1_000_000), ("10m", 10_000_000), ("100m", 100_000_000)] {
            let WorkloadConfig::Synthetic(config) =
                WorkloadConfig::from_file(format!("configs/workload/synthetic-{name}.yml"))
            else {
                panic!("synthetic-{name}.yml is not a synthetic workload");
            };

            assert_eq!(config.total_rows, total_rows);
            assert_eq!(config.select_rows, 1000);
            assert_eq!(config.select_cols, 10);
        }
    }

    #[test]
    fn file_workload_is_parsed_with_optional_nullability() {
        let yaml = r#"
type: file
rows: /data/table
requests: /data/requests.jsonl.gz
schema:
  keys:
    - { name: "6", dtype: utf8 }
  values:
    - { name: "1228", dtype: float64 }
    - { name: "1230", dtype: int64, nullable: false }
"#;

        let WorkloadConfig::File(config) = serde_yaml_ng::from_str(yaml).unwrap() else {
            panic!("expected a file workload");
        };

        assert_eq!(config.rows, Path::new("/data/table"));
        assert_eq!(config.requests, Path::new("/data/requests.jsonl.gz"));
        assert_eq!(config.schema.keys[0].name, "6");
        assert_eq!(config.schema.keys[0].dtype, DType::Utf8);
        assert_eq!(config.schema.values[0].dtype, DType::Float64);
        assert_eq!(config.schema.values[0].nullable, None);
        assert_eq!(config.schema.values[1].dtype, DType::Int64);
        assert_eq!(config.schema.values[1].nullable, Some(false));
    }

    #[test]
    fn unknown_workload_type_is_rejected() {
        let result: Result<WorkloadConfig, _> = serde_yaml_ng::from_str("type: kafka\ntopic: t");

        assert!(result.is_err());
    }
}
