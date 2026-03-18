use std::hint::black_box;
use std::time::Duration;

use arrow::array::AsArray;
use criterion::{BenchmarkId, Criterion, Throughput};
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

        let columns = testdata::column_names(config.select_cols);
        let schema = testdata::make_schema(config.select_cols);
        let num_batches = config.total_rows.div_ceil(config.write_batch_size);
        info!(
            "[{group_name}] writing {} rows in {num_batches} batches...",
            config.total_rows
        );

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
            info!("[{group_name}] wrote batch {}/{num_batches}", i + 1);
        }

        let mem_after = rt.block_on(backend.memory_usage());
        info!("[{group_name}] memory after load:  {:?}", mem_after);
        info!("[{group_name}] memory delta:       {:?}", mem_before.diff(&mem_after));
        let disk_after = rt.block_on(backend.disk_usage());
        info!("[{group_name}] disk after load:    {:?}", disk_after);
        info!("[{group_name}] disk delta:         {:?}", disk_before.diff(&disk_after));

        let total_rows = config.total_rows;
        let select_rows = config.select_rows;

        info!("[{group_name}] starting benchmark...");

        let mut group = c.benchmark_group(format!("{}/rows_{}", group_name, total_rows));
        group.sample_size(config.sample_size);
        group.measurement_time(Duration::from_secs(config.measurement_time_secs));
        group.warm_up_time(Duration::from_secs(config.warmup_time_secs));
        group.throughput(Throughput::Elements(select_rows as u64));

        group.bench_with_input(
            BenchmarkId::new("keys", select_rows),
            &select_rows,
            |b, _| {
                b.to_async(rt).iter(|| {
                    let backend = backend.clone();
                    let columns = columns.clone();
                    async move {
                        let keys = testdata::generate_random_keys(select_rows, total_rows);
                        black_box(backend.read(&keys, &columns).await)
                    }
                })
            },
        );
        group.finish();

        info!("[{group_name}] cleaning up...");
        rt.block_on(backend.cleanup());
        info!("[{group_name}] done");
    }
}
