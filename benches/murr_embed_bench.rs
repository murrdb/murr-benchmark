use criterion::{criterion_group, criterion_main};
use murr_benchmark::backends::murr_embed::MurrEmbed;
use murr_benchmark::bench::Bench;

fn bench_murr_embed(c: &mut criterion::Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    Bench::run::<MurrEmbed>(c, "configs/murr_embed.yaml", "murr_embed", &rt);
}

criterion_group!(benches, bench_murr_embed);
criterion_main!(benches);
