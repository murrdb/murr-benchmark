use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::stats::disk::DiskUsage;
use crate::stats::latency::LatencyStats;
use crate::stats::mem::MemoryUsage;
use crate::stats::net::NetworkUsage;

/// Env var naming the directory reports are written under.
pub const REPORT_DIR_ENV: &str = "REPORT_DIR";
pub const DEFAULT_REPORT_DIR: &str = "reports";

/// Everything measured for one backend variant in one benchmark run.
#[derive(Debug, Serialize)]
pub struct Report {
    pub version: u32,
    /// Run start, UTC, as `YYYY-MM-DDTHH:MM:SSZ`.
    pub timestamp: String,
    pub bench: String,
    pub variant: String,
    pub db: DbReport,
    pub workload: WorkloadReport,
    pub ingest: IngestReport,
    pub memory: MemoryReport,
    pub disk: DiskReport,
    pub network: NetworkReport,
    pub read: ReadReport,
}

#[derive(Debug, Serialize)]
pub struct DbReport {
    pub config_path: String,
    pub write_batch_size: usize,
    pub measurement_time_secs: u64,
    pub warmup_time_secs: u64,
    pub sample_size: usize,
    /// The variant's `backend` entry as written in the YAML.
    pub backend: Value,
}

#[derive(Debug, Serialize)]
pub struct WorkloadReport {
    pub config_path: String,
    /// File stem of `config_path`.
    pub name: String,
    /// The workload YAML as written.
    pub config: Value,
    pub total_rows: usize,
    /// `None` when the number of keys varies between requests.
    pub keys_per_request: Option<usize>,
    pub key_columns: usize,
    pub value_columns: usize,
}

#[derive(Debug, Serialize)]
pub struct IngestReport {
    pub rows: usize,
    pub batches: usize,
    pub elapsed_secs: f64,
    pub rows_per_sec: f64,
    pub flush_secs: f64,
}

#[derive(Debug, Serialize)]
pub struct MemoryReport {
    pub before_load: MemoryUsage,
    pub after_load: MemoryUsage,
    pub after_bench: MemoryUsage,
    pub load_delta: MemoryUsage,
    pub bench_delta: MemoryUsage,
}

#[derive(Debug, Serialize)]
pub struct DiskReport {
    pub before_load: DiskUsage,
    pub after_load: DiskUsage,
    pub load_delta: DiskUsage,
}

#[derive(Debug, Serialize)]
pub struct NetworkReport {
    pub before_load: NetworkUsage,
    pub after_load: NetworkUsage,
    pub after_bench: NetworkUsage,
    pub load_delta: NetworkUsage,
    pub bench_delta: NetworkUsage,
}

#[derive(Debug, Serialize)]
pub struct ReadReport {
    /// All reads including warmup; the denominator of the per-call network figures.
    pub calls_total: u64,
    /// Reads made after warmup; the basis of `latency_ns`.
    pub calls_measured: u64,
    pub latency_ns: Option<LatencyStats>,
    pub rx_bytes_per_call: Option<f64>,
    pub tx_bytes_per_call: Option<f64>,
}

impl Report {
    /// Bumped when the JSON layout changes incompatibly.
    pub const VERSION: u32 = 1;

    /// Current UTC time in the format of `Report::timestamp`.
    pub fn now() -> String {
        jiff::Timestamp::now()
            .strftime("%Y-%m-%dT%H:%M:%SZ")
            .to_string()
    }

    /// `<bench>/<variant>/<workload>-<timestamp>.json`, relative to the report directory.
    pub fn relative_path(&self) -> PathBuf {
        let stamp: String = self
            .timestamp
            .chars()
            .filter(|c| !matches!(c, '-' | ':'))
            .collect();
        Path::new(&self.bench)
            .join(&self.variant)
            .join(format!("{}-{stamp}.json", self.workload.name))
    }

    /// Writes the report as pretty-printed JSON under `dir` and returns the file path.
    pub fn write(&self, dir: &Path) -> PathBuf {
        let path = dir.join(self.relative_path());
        std::fs::create_dir_all(path.parent().unwrap()).expect("failed to create report dir");
        let mut writer = BufWriter::new(File::create(&path).expect("failed to create report file"));
        serde_json::to_writer_pretty(&mut writer, self).expect("failed to write report");
        writer.write_all(b"\n").expect("failed to write report");
        writer.flush().expect("failed to write report");
        path
    }
}

/// Reads a YAML config as an untyped value, for embedding into a report as written.
pub fn read_yaml(path: impl AsRef<Path>) -> Value {
    let contents = std::fs::read_to_string(path).expect("failed to read config file");
    serde_yaml_ng::from_str(&contents).expect("failed to parse config YAML")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn mem(total_bytes: u64) -> MemoryUsage {
        MemoryUsage { rss_bytes: total_bytes, shared_bytes: 0, total_bytes }
    }

    fn net(rx_bytes: u64, tx_bytes: u64) -> NetworkUsage {
        NetworkUsage { rx_bytes, tx_bytes }
    }

    fn report() -> Report {
        Report {
            version: Report::VERSION,
            timestamp: "2026-10-05T19:47:15Z".to_string(),
            bench: "redis_featureblob".to_string(),
            variant: "redis_default".to_string(),
            db: DbReport {
                config_path: "configs/db/redis_featureblob.yaml".to_string(),
                write_batch_size: 100_000,
                measurement_time_secs: 30,
                warmup_time_secs: 2,
                sample_size: 50,
                backend: json!({ "image": "redis:8.10.2" }),
            },
            workload: WorkloadReport {
                config_path: "configs/workload/dh/feat_vendor.yml".to_string(),
                name: "feat_vendor".to_string(),
                config: json!({ "type": "file" }),
                total_rows: 84_670,
                keys_per_request: None,
                key_columns: 2,
                value_columns: 37,
            },
            ingest: IngestReport {
                rows: 84_670,
                batches: 1,
                elapsed_secs: 0.25,
                rows_per_sec: 338_680.0,
                flush_secs: 0.0,
            },
            memory: MemoryReport {
                before_load: mem(100),
                after_load: mem(600),
                after_bench: mem(650),
                load_delta: mem(500),
                bench_delta: mem(50),
            },
            disk: DiskReport {
                before_load: DiskUsage { used_bytes: 0 },
                after_load: DiskUsage { used_bytes: 4096 },
                load_delta: DiskUsage { used_bytes: 4096 },
            },
            network: NetworkReport {
                before_load: net(0, 0),
                after_load: net(1000, 10),
                after_bench: net(3000, 90_010),
                load_delta: net(1000, 10),
                bench_delta: net(2000, 90_000),
            },
            read: ReadReport {
                calls_total: 2,
                calls_measured: 1,
                latency_ns: Some(LatencyStats {
                    mean: 104_000.0,
                    min: 104_000,
                    p50: 104_000,
                    p90: 104_000,
                    p99: 104_000,
                    p999: 104_000,
                    max: 104_000,
                }),
                rx_bytes_per_call: Some(1000.0),
                tx_bytes_per_call: Some(45_000.0),
            },
        }
    }

    #[test]
    fn report_is_written_under_bench_and_variant_dirs() {
        let dir = tempfile::tempdir().unwrap();

        let path = report().write(dir.path());

        assert_eq!(
            path,
            dir.path()
                .join("redis_featureblob/redis_default/feat_vendor-20261005T194715Z.json")
        );
        assert!(path.is_file());
    }

    #[test]
    fn written_report_carries_the_readme_numbers() {
        let dir = tempfile::tempdir().unwrap();

        let path = report().write(dir.path());
        let json: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();

        assert_eq!(json["version"], 1);
        assert_eq!(json["db"]["backend"]["image"], "redis:8.10.2");
        assert_eq!(json["memory"]["load_delta"]["total_bytes"], 500);
        assert_eq!(json["disk"]["load_delta"]["used_bytes"], 4096);
        assert_eq!(json["ingest"]["rows_per_sec"], 338_680.0);
        assert_eq!(json["read"]["latency_ns"]["p50"], 104_000);
        assert_eq!(json["read"]["tx_bytes_per_call"], 45_000.0);
        assert_eq!(json["workload"]["keys_per_request"], Value::Null);
    }

    #[test]
    fn yaml_config_is_read_as_written() {
        let value = read_yaml("configs/workload/synthetic-1m.yml");

        assert_eq!(
            value,
            json!({ "type": "synthetic", "total_rows": 1_000_000, "select_rows": 1000, "select_cols": 10 })
        );
    }
}
