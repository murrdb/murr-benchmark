# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Benchmarking suite for [murr](https://github.com/murrdb/murr) — a columnar in-memory cache for AI/ML inference workloads. Two complementary harnesses: **Rust (Criterion)** measures raw service throughput (time-to-last-byte, no decoding), **Python (pyperf)** measures end-to-end latency including DataFrame conversion. Both share the same YAML configs and test data generation logic.

## Build & Benchmark Commands

```bash
cargo bench                      # run all benchmarks
cargo bench --bench <name>       # run a single benchmark
cargo bench --bench <name> -- --help  # criterion CLI options
cargo test                       # run tests
```

```bash
# Python harness (from repo root)
cd python && uv sync && cd ..
uv run --project python murr-bench <backend_name>
uv run --project python murr-bench <backend_name> --config configs/<name>.yaml
cd python && uv run pytest tests/ -v  # python tests
```

Benchmark names: `murrdb_http`, `murr_embed`, `redis_feast`, `redis_featureblob`, `rocksdb`, `pg_feast`, `pg_featureblob`.

Container-backed benchmarks require Docker running locally (`testcontainers` manages lifecycle). `murr_embed` and `rocksdb` are in-process and do not need Docker.

## Architecture

Rust library code in `src/`, thin Criterion wrappers in `benches/`, Python harness in `python/`, per-backend YAML configs in `configs/`.

**Core modules:**

- `src/config.rs` — `BenchConfig<B>` generic over `BackendConfig` trait, deserialized from YAML.
- `src/backend.rs` — `Backend` trait with async `init`, `write_batch`, `read`, `flush`, `memory_usage`, `disk_usage`, `cleanup`. Generic (not object-safe), each backend has associated `Config` and `Response` types.
- `src/bench.rs` — `Bench::run::<B>()` orchestrator: loads config, inits backend, generates test data batches, measures memory/disk before and after load, runs Criterion benchmark with randomized keys per iteration.
- `src/testdata.rs` — Arrow RecordBatch data generator (schema builder, batch iterator, random key generator).
- `src/stats/` — `MemoryUsage` (Docker stats API or `/proc/self/statm`) and `DiskUsage` (Docker inspect or recursive directory walk) with `diff()` for before/after deltas.
- `src/testing.rs` — Shared integration test helpers.
- `src/backends/` — Backend implementations.

**Backends:**

- `murr_http` — Dockerized murrdb over HTTP + Arrow IPC.
- `murr_embed` — In-process `MurrService` (depends on `murr` crate with `testutil` feature). No Docker.
- `redis_feast` — Redis with Feast-style HSETs. Configurable read mode (`hgetall` or `hmget`).
- `redis_featureblob` — Redis with compact byte-blob encoding (`SET`/`MGET`).
- `rocksdb` — Local RocksDB key-value store. No Docker.
- `pg_feast` — PostgreSQL with explicit typed `REAL` columns per feature.
- `pg_featureblob` — PostgreSQL with single `BYTEA` blob column.

Redis backends share a `RedisContainer`; PostgreSQL backends share a `PgContainer`.

## Key Patterns

- **Criterion** is the benchmarking framework. Benchmarks use `criterion_group!`/`criterion_main!` macros with `harness = false` in Cargo.toml. Async benchmarks use `b.to_async(rt)`.
- **Arrow RecordBatch** is the data interchange format for test data generation and murrdb writes.
- **Randomized keys** — each benchmark iteration generates fresh random keys to avoid caching effects.
- **BackendConfig trait** — each backend defines its own config struct; the generic `BenchConfig<B>` ensures type safety.
- Bench profile enables `debug = 1`, `codegen-units = 1`, and `force-frame-pointers = true` for profiling support.

## Configuration

YAML config files in `configs/` control all benchmark parameters (shared by both Rust and Python harnesses):

- `total_rows` — number of rows in the test dataset
- `select_rows` — number of keys to look up per iteration
- `select_cols` — number of Float32 columns (always explicit)
- `write_batch_size` — RecordBatch chunk size for data loading
- `measurement_time_secs`, `warmup_time_secs`, `sample_size` — Criterion settings
- `backend` — per-backend settings (image, read_mode, data_dir, etc.)
