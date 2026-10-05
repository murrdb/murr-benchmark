use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use log::info;
use tokio::runtime::Runtime;

use crate::backend::Backend;
use crate::config::{DbSuite, WorkloadConfig};
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
            info!("[{label}] flush total: {:.2?}", flush_start.elapsed());

            let mem_after = rt.block_on(backend.memory_usage());
            info!("[{label}] memory after load:  {:?}", mem_after);
            info!("[{label}] memory delta:       {:?}", mem_before.diff(&mem_after));
            let disk_after = rt.block_on(backend.disk_usage());
            info!("[{label}] disk after load:    {:?}", disk_after);
            info!("[{label}] disk delta:         {:?}", disk_before.diff(&disk_after));
            let net_after = rt.block_on(backend.network_usage());
            info!("[{label}] net after load:     {:?}", net_after);
            info!("[{label}] net delta:          {:?}", net_before.diff(&net_after));

            info!("[{label}] starting benchmark...");

            let mut group = c.benchmark_group(format!("{}/rows_{}", label, total_rows));
            group.sample_size(config.sample_size);
            group.measurement_time(Duration::from_secs(config.measurement_time_secs));
            group.warm_up_time(Duration::from_secs(config.warmup_time_secs));
            group.throughput(throughput.clone());

            let read_count = Arc::new(AtomicU64::new(0));
            let mut requests = workload.requests();

            group.bench_function(benchmark_id.clone(), |b| {
                b.to_async(rt).iter_batched(
                    || requests.next().expect("workload ran out of requests"),
                    |request| {
                        let backend = backend.clone();
                        let read_count = read_count.clone();
                        async move {
                            let resp = black_box(backend.read(&request).await);
                            read_count.fetch_add(1, Ordering::Relaxed);
                            resp
                        }
                    },
                    BatchSize::SmallInput,
                )
            });
            group.finish();

            let mem_bench = rt.block_on(backend.memory_usage());
            info!("[{label}] memory after bench: {:?}", mem_bench);
            info!("[{label}] memory delta (bench): {:?}", mem_after.diff(&mem_bench));
            let net_bench = rt.block_on(backend.network_usage());
            let net_delta_bench = net_after.diff(&net_bench);
            info!("[{label}] net after bench:    {:?}", net_bench);
            info!("[{label}] net delta (bench):  {:?}", net_delta_bench);

            let reads = read_count.load(Ordering::Relaxed);
            info!("[{label}] reads:              {reads} calls");
            if reads > 0 {
                let rx_per_call = net_delta_bench.rx_bytes as f64 / reads as f64;
                let tx_per_call = net_delta_bench.tx_bytes as f64 / reads as f64;
                info!(
                    "[{label}] net per read:       RX={:.1} bytes/call, TX={:.1} bytes/call",
                    rx_per_call, tx_per_call
                );
            }

            info!("[{label}] cleaning up...");
            rt.block_on(backend.cleanup());
            info!("[{label}] done");
        }
    }
}
