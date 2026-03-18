use criterion::{criterion_group, criterion_main};
use murr_benchmark::backends::redis::featureblob::RedisFeatureBlob;
use murr_benchmark::bench::Bench;

fn bench_redis_featureblob(c: &mut criterion::Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    Bench::run::<RedisFeatureBlob>(c, "configs/redis_featureblob.yaml", "redis_featureblob", &rt);
}

criterion_group!(benches, bench_redis_featureblob);
criterion_main!(benches);
