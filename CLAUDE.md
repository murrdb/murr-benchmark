# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Benchmarking suite for [murr](https://github.com/murrdb/murr) — a columnar in-memory cache for AI/ML inference workloads. Compares murr's read performance against Redis baselines. All backends run as Docker containers via `testcontainers`.

## Build & Benchmark Commands

```bash
cargo bench                      # run all benchmarks
cargo bench --bench <name>       # run a single benchmark
cargo bench --bench <name> -- --help  # criterion CLI options
```

Benchmark names: `murrdb_http`, `murr_embed`, `redis_feast`, `redis_featureblob`, `rocksdb`, `pg_feast`, `pg_featureblob`.

All benchmarks require Docker running locally — they use `testcontainers` to spin up backend containers.

## Architecture

Library code lives in `src/`, thin Criterion wrappers in `benches/`, per-backend YAML configs in `configs/`.

**Core modules:**

- `src/config.rs` — `BenchConfig<B>` generic over `BackendConfig` trait, deserialized from YAML.
- `src/backend.rs` — `Backend` trait with async `init`, `write_batch`, `read`, `cleanup`. Generic (not object-safe), each backend has associated `Config` and `Response` types.
- `src/bench.rs` — `Bench::run::<B>()` orchestrator: loads config, inits backend, generates test data batches, runs Criterion benchmark with randomized keys per iteration.
- `src/testdata.rs` — Generic Arrow RecordBatch data generator (schema builder, batch iterator, random key generator). No external service dependencies.
- `src/backends/` — Backend implementations.

**Backends:**

- `murr_http` — Dockerized murrdb (`ghcr.io/murrdb/murr:latest`, port 8080). Writes via Arrow IPC, reads via JSON request + Arrow IPC response.
- `redis_feast` — Redis with Feast-style HSETs. Configurable read mode (`hgetall` or `hmget`).
- `redis_featureblob` — Redis with compact byte-blob encoding (`SET`/`MGET`).

**Design docs:** See `specs/design.md` for detailed design decisions.

## Key Patterns

- **Criterion** is the benchmarking framework. Benchmarks use `criterion_group!`/`criterion_main!` macros with `harness = false` in Cargo.toml. Async benchmarks use `b.to_async(rt)`.
- **Arrow RecordBatch** is the data interchange format for test data generation and murrdb writes.
- **Randomized keys** — each benchmark iteration generates fresh random keys to avoid caching effects.
- **No murr crate dependency** — murrdb is accessed only via its Docker container's HTTP API.
- **BackendConfig trait** — each backend defines its own config struct; the generic `BenchConfig<B>` ensures type safety.
- Bench profile enables `debug = 1` and `codegen-units = 1` for profiling support.

## Configuration

YAML config files in `configs/` control all benchmark parameters:

- `total_rows` — number of rows in the test dataset
- `select_rows` — number of keys to look up per iteration
- `select_cols` — number of Float32 columns (always explicit)
- `write_batch_size` — RecordBatch chunk size for data loading
- `measurement_time_secs`, `warmup_time_secs`, `sample_size` — Criterion settings
- `backend` — per-backend settings (image, read_mode, etc.)
