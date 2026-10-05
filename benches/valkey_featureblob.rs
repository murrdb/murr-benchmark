use criterion::{criterion_group, criterion_main};
use murr_benchmark::backends::redis::featureblob::RedisFeatureBlob;
use murr_benchmark::bench::Bench;

fn bench_valkey_featureblob(c: &mut criterion::Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    Bench::run::<RedisFeatureBlob>(c, "configs/db/valkey_featureblob.yaml", "valkey_featureblob", &rt);
}

criterion_group!(benches, bench_valkey_featureblob);
criterion_main!(benches);
