use criterion::{criterion_group, criterion_main};
use murr_benchmark::backends::rocksdb::RocksDb;
use murr_benchmark::bench::Bench;

fn bench_rocksdb(c: &mut criterion::Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    Bench::run::<RocksDb>(c, "configs/rocksdb.yaml", "rocksdb", &rt);
}

criterion_group!(benches, bench_rocksdb);
criterion_main!(benches);
