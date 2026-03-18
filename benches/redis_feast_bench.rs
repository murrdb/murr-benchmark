use criterion::{criterion_group, criterion_main};
use murr_benchmark::backends::redis::feast::RedisFeast;
use murr_benchmark::bench::Bench;

fn bench_redis_feast(c: &mut criterion::Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    Bench::run::<RedisFeast>(c, "configs/redis_feast.yaml", "redis_feast", &rt);
}

criterion_group!(benches, bench_redis_feast);
criterion_main!(benches);
