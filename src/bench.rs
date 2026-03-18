use std::hint::black_box;
use std::time::Duration;

use arrow::array::AsArray;
use criterion::{BenchmarkId, Criterion, Throughput};
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
        let config = crate::config::BenchConfig::<B::Config>::from_file(config_path);

        // 1. Init backend
        let backend = rt.block_on(B::init(&config));

        // 2. Generate and write test data in batches
        let columns = testdata::column_names(config.select_cols);
        let schema = testdata::make_schema(config.select_cols);
        for record_batch in
            testdata::generate_batches(&schema, config.total_rows, config.write_batch_size)
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
        }

        let total_rows = config.total_rows;
        let select_rows = config.select_rows;

        // 3. Criterion benchmark
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

        // 4. Cleanup
        rt.block_on(backend.cleanup());
    }
}
