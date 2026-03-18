use criterion::{criterion_group, criterion_main};
use murr_benchmark::backends::postgres::feast::PgFeast;
use murr_benchmark::bench::Bench;

fn bench_pg_feast(c: &mut criterion::Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    Bench::run::<PgFeast>(c, "configs/pg_feast.yaml", "pg_feast", &rt);
}

criterion_group!(benches, bench_pg_feast);
criterion_main!(benches);
