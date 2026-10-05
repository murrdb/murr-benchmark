use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use log::info;
use tokio::runtime::Runtime;

use crate::backend::Backend;
use crate::config::{DbSuite, WorkloadConfig};
use crate::report::{
    self, DbReport, DiskReport, IngestReport, MemoryReport, NetworkReport, ReadReport, Report,
    WorkloadReport,
};
use crate::stats::latency::LatencyRecorder;
use crate::workload::{Row, RowBatch};

/// Env var naming the workload YAML; the db config path is fixed per bench target.
const WORKLOAD_ENV: &str = "WORKLOAD";
const DEFAULT_WORKLOAD: &str = "configs/workload/synthetic-1m.yml";

pub struct Bench;

impl Bench {
    pub fn run<B: Backend + 'static>(
        c: &mut Criterion,
        config_path: &str,
        group_name: &str,
        rt: &Runtime,
    ) {
        let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .try_init();

        let suite = DbSuite::<B::Config>::from_file(config_path);
        let workload_path =
            std::env::var(WORKLOAD_ENV).unwrap_or_else(|_| DEFAULT_WORKLOAD.to_string());
        let workload_config = WorkloadConfig::from_file(&workload_path);

        let timestamp = Report::now();
        let report_dir = PathBuf::from(
            std::env::var(report::REPORT_DIR_ENV)
                .unwrap_or_else(|_| report::DEFAULT_REPORT_DIR.to_string()),
        );
        let db_yaml = report::read_yaml(config_path);
        let workload_yaml = report::read_yaml(&workload_path);
        let workload_name = Path::new(&workload_path)
            .file_stem()
            .expect("workload path has no file name")
            .to_string_lossy()
            .into_owned();

        info!("[{group_name}] config: {config_path}");
        info!("[{group_name}] workload: {workload_path} {workload_config:?}");
        let workload = workload_config.build();
        let schema = workload.schema();
        let total_rows = workload.total_rows();
        // A fixed key count is measured per key; requests of varying size are measured per request.
        let (benchmark_id, throughput) = match workload.keys_per_request() {
            Some(keys) => (BenchmarkId::new("keys", keys), Throughput::Elements(keys as u64)),
            None => (BenchmarkId::from_parameter("replay"), Throughput::Elements(1)),
        };
        info!(
            "[{group_name}] total_rows={total_rows}, keys_per_request={:?}, write_batch_size={}",
            workload.keys_per_request(),
            suite.write_batch_size
        );
        info!(
            "[{group_name}] measurement={}s, warmup={}s, samples={}",
            suite.measurement_time_secs, suite.warmup_time_secs, suite.sample_size
        );
        info!(
            "[{group_name}] variants: {}",
            suite
                .backend
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        );

        for (variant_name, config) in suite.variants() {
            let label = format!("{group_name}/{variant_name}");
            info!("[{label}] backend: {:?}", config.backend);

            // testcontainers' async Drop calls Handle::current(); without an
            // entered runtime, a panic anywhere below would unwind into the
            // container's drop and abort with "no reactor running".
            let _rt_guard = rt.enter();

            info!("[{label}] initializing backend...");
            let backend = rt.block_on(B::init(&config, schema.clone()));
            info!("[{label}] backend ready");

            let mem_before = rt.block_on(backend.memory_usage());
            info!("[{label}] memory before load: {:?}", mem_before);
            let disk_before = rt.block_on(backend.disk_usage());
            info!("[{label}] disk before load:   {:?}", disk_before);
            let net_before = rt.block_on(backend.network_usage());
            info!("[{label}] net before load:    {:?}", net_before);

            let num_batches = total_rows.div_ceil(config.write_batch_size);
            info!("[{label}] writing {total_rows} rows in {num_batches} batches...");

            let ingest_start = Instant::now();
            let mut last_log = Instant::now();
            let mut rows = workload.rows();
            let mut written = 0;
            loop {
                let chunk: Vec<Row> = rows.by_ref().take(config.write_batch_size).collect();
                if chunk.is_empty() {
                    break;
                }
                rt.block_on(backend.write_batch(&RowBatch { rows: chunk }));
                written += 1;
                if written == num_batches || last_log.elapsed() >= Duration::from_secs(5) {
                    info!("[{label}] wrote batch {written}/{num_batches}");
                    last_log = Instant::now();
                }
            }
            drop(rows);

            let ingest_elapsed = ingest_start.elapsed();
            info!(
                "[{label}] ingest total: {:.2?} ({:.0} rows/s)",
                ingest_elapsed,
                total_rows as f64 / ingest_elapsed.as_secs_f64()
            );

            info!("[{label}] flushing...");
            let flush_start = Instant::now();
            rt.block_on(backend.flush());
            let flush_elapsed = flush_start.elapsed();
            info!("[{label}] flush total: {:.2?}", flush_elapsed);

            let mem_after = rt.block_on(backend.memory_usage());
            let mem_delta_load = mem_before.diff(&mem_after);
            info!("[{label}] memory after load:  {:?}", mem_after);
            info!("[{label}] memory delta:       {:?}", mem_delta_load);
            let disk_after = rt.block_on(backend.disk_usage());
            let disk_delta_load = disk_before.diff(&disk_after);
            info!("[{label}] disk after load:    {:?}", disk_after);
            info!("[{label}] disk delta:         {:?}", disk_delta_load);
            let net_after = rt.block_on(backend.network_usage());
            let net_delta_load = net_before.diff(&net_after);
            info!("[{label}] net after load:     {:?}", net_after);
            info!("[{label}] net delta:          {:?}", net_delta_load);

            info!("[{label}] starting benchmark...");

            let mut group = c.benchmark_group(format!("{}/rows_{}", label, total_rows));
            group.sample_size(config.sample_size);
            group.measurement_time(Duration::from_secs(config.measurement_time_secs));
            group.warm_up_time(Duration::from_secs(config.warmup_time_secs));
            group.throughput(throughput.clone());

            let read_count = Arc::new(AtomicU64::new(0));
            let latency = Arc::new(LatencyRecorder::new(Duration::from_secs(
                config.warmup_time_secs,
            )));
            let mut requests = workload.requests();

            group.bench_function(benchmark_id.clone(), |b| {
                b.to_async(rt).iter_batched(
                    || requests.next().expect("workload ran out of requests"),
                    |request| {
                        let backend = backend.clone();
                        let read_count = read_count.clone();
                        let latency = latency.clone();
                        async move {
                            let start = Instant::now();
                            let resp = black_box(backend.read(&request).await);
                            latency.record(start, start.elapsed());
                            read_count.fetch_add(1, Ordering::Relaxed);
                            resp
                        }
                    },
                    BatchSize::SmallInput,
                )
            });
            group.finish();

            let mem_bench = rt.block_on(backend.memory_usage());
            let mem_delta_bench = mem_after.diff(&mem_bench);
            info!("[{label}] memory after bench: {:?}", mem_bench);
            info!("[{label}] memory delta (bench): {:?}", mem_delta_bench);
            let net_bench = rt.block_on(backend.network_usage());
            let net_delta_bench = net_after.diff(&net_bench);
            info!("[{label}] net after bench:    {:?}", net_bench);
            info!("[{label}] net delta (bench):  {:?}", net_delta_bench);

            let reads = read_count.load(Ordering::Relaxed);
            info!("[{label}] reads:              {reads} calls");
            let net_per_call = (reads > 0).then(|| {
                (
                    net_delta_bench.rx_bytes as f64 / reads as f64,
                    net_delta_bench.tx_bytes as f64 / reads as f64,
                )
            });
            if let Some((rx_per_call, tx_per_call)) = net_per_call {
                info!(
                    "[{label}] net per read:       RX={:.1} bytes/call, TX={:.1} bytes/call",
                    rx_per_call, tx_per_call
                );
            }
            let latency_stats = latency.stats();
            if let Some(stats) = &latency_stats {
                info!(
                    "[{label}] read latency:       p50={:.2?}, p99={:.2?}, max={:.2?} ({} calls after warmup)",
                    Duration::from_nanos(stats.p50),
                    Duration::from_nanos(stats.p99),
                    Duration::from_nanos(stats.max),
                    latency.len()
                );
            }

            let report = Report {
                version: Report::VERSION,
                timestamp: timestamp.clone(),
                bench: group_name.to_string(),
                variant: variant_name.clone(),
                db: DbReport {
                    config_path: config_path.to_string(),
                    write_batch_size: config.write_batch_size,
                    measurement_time_secs: config.measurement_time_secs,
                    warmup_time_secs: config.warmup_time_secs,
                    sample_size: config.sample_size,
                    backend: db_yaml["backend"][&variant_name].clone(),
                },
                workload: WorkloadReport {
                    config_path: workload_path.clone(),
                    name: workload_name.clone(),
                    config: workload_yaml.clone(),
                    total_rows,
                    keys_per_request: workload.keys_per_request(),
                    key_columns: schema.keys.len(),
                    value_columns: schema.values.len(),
                },
                ingest: IngestReport {
                    rows: total_rows,
                    batches: written,
                    elapsed_secs: ingest_elapsed.as_secs_f64(),
                    rows_per_sec: total_rows as f64 / ingest_elapsed.as_secs_f64(),
                    flush_secs: flush_elapsed.as_secs_f64(),
                },
                memory: MemoryReport {
                    before_load: mem_before,
                    after_load: mem_after,
                    after_bench: mem_bench,
                    load_delta: mem_delta_load,
                    bench_delta: mem_delta_bench,
                },
                disk: DiskReport {
                    before_load: disk_before,
                    after_load: disk_after,
                    load_delta: disk_delta_load,
                },
                network: NetworkReport {
                    before_load: net_before,
                    after_load: net_after,
                    after_bench: net_bench,
                    load_delta: net_delta_load,
                    bench_delta: net_delta_bench,
                },
                read: ReadReport {
                    calls_total: reads,
                    calls_measured: latency.len() as u64,
                    latency_ns: latency_stats,
                    rx_bytes_per_call: net_per_call.map(|(rx, _)| rx),
                    tx_bytes_per_call: net_per_call.map(|(_, tx)| tx),
                },
            };
            let report_path = report.write(&report_dir);
            info!("[{label}] report:             {}", report_path.display());

            info!("[{label}] cleaning up...");
            rt.block_on(backend.cleanup());
            info!("[{label}] done");
        }
    }
}
