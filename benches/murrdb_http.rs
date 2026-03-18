use criterion::{criterion_group, criterion_main};
use murr_benchmark::backends::murr_http::MurrHttp;
use murr_benchmark::bench::Bench;

fn bench_http(c: &mut criterion::Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    Bench::run::<MurrHttp>(c, "configs/murr_http.yaml", "murr_http", &rt);
}

criterion_group!(benches, bench_http);
criterion_main!(benches);
