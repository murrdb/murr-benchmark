use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::AsArray;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use log::info;
use tokio::runtime::Runtime;

use crate::backend::{Backend, Batch};
use crate::testdata;

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

        let config = crate::config::BenchConfig::<B::Config>::from_file(config_path);

        info!("[{group_name}] config: {config_path}");
        info!(
            "[{group_name}] total_rows={}, select_rows={}, select_cols={}, write_batch_size={}",
            config.total_rows, config.select_rows, config.select_cols, config.write_batch_size
        );
        info!(
            "[{group_name}] measurement={}s, warmup={}s, samples={}",
            config.measurement_time_secs, config.warmup_time_secs, config.sample_size
        );
        info!("[{group_name}] backend: {:?}", config.backend);

        info!("[{group_name}] initializing backend...");
        let backend = rt.block_on(B::init(&config));
        info!("[{group_name}] backend ready");

        let mem_before = rt.block_on(backend.memory_usage());
        info!("[{group_name}] memory before load: {:?}", mem_before);
        let disk_before = rt.block_on(backend.disk_usage());
        info!("[{group_name}] disk before load:   {:?}", disk_before);
        let net_before = rt.block_on(backend.network_usage());
        info!("[{group_name}] net before load:    {:?}", net_before);

        let columns = testdata::column_names(config.select_cols);
        let schema = testdata::make_schema(config.select_cols);
        let num_batches = config.total_rows.div_ceil(config.write_batch_size);
        info!(
            "[{group_name}] writing {} rows in {num_batches} batches...",
            config.total_rows
        );

        let ingest_start = Instant::now();
        let mut last_log = Instant::now();
        for (i, record_batch) in
            testdata::generate_batches(&schema, config.total_rows, config.write_batch_size)
                .enumerate()
        {
            let keys: Vec<String> = record_batch
                .column(0)
                .as_string::<i32>()
                .iter()
                .map(|v| v.unwrap().to_string())
                .collect();
            let batch = Batch {
                inner: record_batch,
                keys,
                columns: columns.clone(),
            };
            rt.block_on(backend.write_batch(&batch));
            if i + 1 == num_batches || last_log.elapsed() >= Duration::from_secs(5) {
                info!("[{group_name}] wrote batch {}/{num_batches}", i + 1);
                last_log = Instant::now();
            }
        }

        let ingest_elapsed = ingest_start.elapsed();
        info!(
            "[{group_name}] ingest total: {:.2?} ({:.0} rows/s)",
            ingest_elapsed,
            config.total_rows as f64 / ingest_elapsed.as_secs_f64()
        );

        info!("[{group_name}] flushing...");
        let flush_start = Instant::now();
        rt.block_on(backend.flush());
        info!("[{group_name}] flush total: {:.2?}", flush_start.elapsed());

        let mem_after = rt.block_on(backend.memory_usage());
        info!("[{group_name}] memory after load:  {:?}", mem_after);
        info!("[{group_name}] memory delta:       {:?}", mem_before.diff(&mem_after));
        let disk_after = rt.block_on(backend.disk_usage());
        info!("[{group_name}] disk after load:    {:?}", disk_after);
        info!("[{group_name}] disk delta:         {:?}", disk_before.diff(&disk_after));
        let net_after = rt.block_on(backend.network_usage());
        info!("[{group_name}] net after load:     {:?}", net_after);
        info!("[{group_name}] net delta:          {:?}", net_before.diff(&net_after));

        let total_rows = config.total_rows;
        let select_rows = config.select_rows;

        info!("[{group_name}] starting benchmark...");

        let mut group = c.benchmark_group(format!("{}/rows_{}", group_name, total_rows));
        group.sample_size(config.sample_size);
        group.measurement_time(Duration::from_secs(config.measurement_time_secs));
        group.warm_up_time(Duration::from_secs(config.warmup_time_secs));
        group.throughput(Throughput::Elements(select_rows as u64));

        let read_count = Arc::new(AtomicU64::new(0));

        group.bench_with_input(
            BenchmarkId::new("keys", select_rows),
            &select_rows,
            |b, _| {
                b.to_async(rt).iter_batched(
                    || testdata::generate_random_keys(select_rows, total_rows),
                    |keys| {
                        let backend = backend.clone();
                        let columns = columns.clone();
                        let read_count = read_count.clone();
                        async move {
                            let resp = black_box(backend.read(&keys, &columns).await);
                            read_count.fetch_add(1, Ordering::Relaxed);
                            resp
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );
        group.finish();

        let mem_bench = rt.block_on(backend.memory_usage());
        info!("[{group_name}] memory after bench: {:?}", mem_bench);
        info!("[{group_name}] memory delta (bench): {:?}", mem_after.diff(&mem_bench));
        let net_bench = rt.block_on(backend.network_usage());
        let net_delta_bench = net_after.diff(&net_bench);
        info!("[{group_name}] net after bench:    {:?}", net_bench);
        info!("[{group_name}] net delta (bench):  {:?}", net_delta_bench);

        let reads = read_count.load(Ordering::Relaxed);
        info!("[{group_name}] reads:              {reads} calls");
        if reads > 0 {
            let rx_per_call = net_delta_bench.rx_bytes as f64 / reads as f64;
            let tx_per_call = net_delta_bench.tx_bytes as f64 / reads as f64;
            info!(
                "[{group_name}] net per read:       RX={:.1} bytes/call, TX={:.1} bytes/call",
                rx_per_call, tx_per_call
            );
        }

        info!("[{group_name}] cleaning up...");
        rt.block_on(backend.cleanup());
        info!("[{group_name}] done");
    }
}
