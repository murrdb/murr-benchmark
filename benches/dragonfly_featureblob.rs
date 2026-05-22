use criterion::{criterion_group, criterion_main};
use murr_benchmark::backends::redis::featureblob::RedisFeatureBlob;
use murr_benchmark::bench::Bench;

fn bench_dragonfly_featureblob(c: &mut criterion::Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    Bench::run::<RedisFeatureBlob>(c, "configs/dragonfly_featureblob.yaml", "dragonfly_featureblob", &rt);
}

criterion_group!(benches, bench_dragonfly_featureblob);
criterion_main!(benches);
