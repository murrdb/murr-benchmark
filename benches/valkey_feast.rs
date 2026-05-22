use criterion::{criterion_group, criterion_main};
use murr_benchmark::backends::redis::feast::RedisFeast;
use murr_benchmark::bench::Bench;

fn bench_valkey_feast(c: &mut criterion::Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    Bench::run::<RedisFeast>(c, "configs/valkey_feast.yaml", "valkey_feast", &rt);
}

criterion_group!(benches, bench_valkey_feast);
criterion_main!(benches);
