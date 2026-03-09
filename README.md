# murr-benchmark

Benchmarks for [murr](https://github.com/murrdb/murr) — a columnar in-memory cache for AI/ML inference workloads.

## Benchmarks

| Benchmark | Description |
|-----------|-------------|
| `table_bench` | Murr table service read performance (10M rows, 1000 keys) |
| `http_bench` | Murr HTTP API (Axum) fetch performance |
| `flight_bench` | Murr Arrow Flight gRPC fetch performance |
| `hashmap_bench` | Column-oriented in-memory hash table baseline |
| `hashmap_row_bench` | Row-oriented and byte-blob storage baselines |
| `redis_feast_bench` | Redis comparison using Feast-style HSETs |
| `redis_featureblob_bench` | Redis comparison using compact feature blobs |

## Running

```bash
cargo bench --bench <name>
```

Redis benchmarks require Docker (uses `testcontainers`).

## License

Apache-2.0 — see [LICENSE](LICENSE).
