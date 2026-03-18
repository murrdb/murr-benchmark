use criterion::{criterion_group, criterion_main};
use murr_benchmark::backends::postgres::featureblob::PgFeatureBlob;
use murr_benchmark::bench::Bench;

fn bench_pg_featureblob(c: &mut criterion::Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    Bench::run::<PgFeatureBlob>(c, "configs/pg_featureblob.yaml", "pg_featureblob", &rt);
}

criterion_group!(benches, bench_pg_featureblob);
criterion_main!(benches);
