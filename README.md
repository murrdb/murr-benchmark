# murr-benchmark

Benchmarks for [murr](https://github.com/murrdb/murr) — a columnar in-memory cache for AI/ML inference workloads.

## Benchmark methodology

The suite includes two complementary harnesses measuring different aspects of read performance:

**Rust (Criterion)** — measures raw service throughput as time-to-last-byte. The benchmark reads `select_rows` random keys from the backend per iteration and consumes the raw response bytes without decoding. This isolates the storage/network layer and shows the theoretical ceiling of each backend.

**Python (pyperf)** — measures end-to-end latency as experienced by a Python ML client. The benchmark performs the same random-key reads but includes full protocol decoding and conversion into a `pd.DataFrame`. This captures the real cost a user pays: HTTP/Redis/SQL protocol parsing, byte deserialization, and DataFrame construction.

Both harnesses share the same YAML config files and test data generation logic (random float32 columns with string keys), so results are directly comparable.

## Backends

| Backend | Transport | Container | Description |
|---------|-----------|-----------|-------------|
| `murrdb_http` | HTTP + Arrow IPC | `ghcr.io/murrdb/murr` | Murr columnar cache over its HTTP API |
| `murr_embed` | In-process | — | Murr embedded library (Rust only) |
| `redis_feast` | Redis protocol | `redis` | Redis with Feast-style hash sets |
| `redis_featureblob` | Redis protocol | `redis` | Redis with compact byte blobs |
| `rocksdb` | In-process | — | Local RocksDB key-value store |
| `pg_feast` | PostgreSQL protocol | `postgres` | PostgreSQL with explicit typed columns |
| `pg_featureblob` | PostgreSQL protocol | `postgres` | PostgreSQL with BYTEA blob column |

All container-backed backends use `testcontainers` to manage Docker lifecycle automatically.

## Data layouts

### Feast (hash-per-row)

Used by `redis_feast` and `pg_feast`. Each entity key maps to a set of individually named feature columns. In Redis this is an HSET with one field per feature; in PostgreSQL it is a table with explicit `REAL` columns.

```
key="42" -> { col_0: 0.71, col_1: 0.33, col_2: 0.89, ... }
```

This layout mirrors [Feast](https://feast.dev/) online store format. It allows reading a subset of columns but has per-field overhead.

### Feature blob (packed binary)

Used by `redis_featureblob`, `pg_featureblob`, and `rocksdb`. All feature values for an entity are concatenated into a single byte buffer of little-endian float32 values.

```
key="42" -> b"\xcd\xcc\x34\x3f\xa4\x70\xa8\x3e..."  (N × 4 bytes)
```

Compact and cache-friendly — a single read returns all features. The client unpacks with `np.frombuffer(blob, dtype='<f4')`. No per-column overhead, but always reads all columns.

### Arrow IPC (columnar)

Used by `murrdb_http`. Data is exchanged as Apache Arrow IPC streams — a columnar binary format with zero-copy read support. Writes send `RecordBatch` via Arrow stream format; reads return the same.

```
POST /api/v1/table/bench/fetch  ->  Arrow IPC stream (RecordBatch)
```

Native format for murr. Preserves column types and supports projection pushdown on the server.

## Config file format

All benchmarks are configured via YAML files in `configs/`. Both the Rust and Python harnesses read the same files.

Example (`configs/redis_featureblob.yaml`):

```yaml
total_rows: 10000000        # total rows loaded into the backend
select_rows: 1000           # number of random keys to read per iteration
select_cols: 10             # number of Float32 feature columns
write_batch_size: 100000    # rows per write batch during data loading
measurement_time_secs: 10   # minimum measurement duration
warmup_time_secs: 2         # warmup duration before measurement
sample_size: 10             # number of measured samples
backend:
  image: "redis:8.6.1"      # Docker image (container-backed backends)
```

Backend-specific fields:
- `backend.image` — Docker image for container-backed backends
- `backend.read_mode` — `hgetall` or `hmget` (redis_feast only)
- `backend.data_dir` — local data directory (rocksdb only)

## Running benchmarks

All benchmarks require Docker running locally (except `murr_embed` and `rocksdb`).

### Rust

```bash
# run all benchmarks
cargo bench

# run a single benchmark
cargo bench --bench redis_featureblob

# available benchmarks
cargo bench --bench murrdb_http
cargo bench --bench murr_embed
cargo bench --bench redis_feast
cargo bench --bench redis_featureblob
cargo bench --bench rocksdb
cargo bench --bench pg_feast
cargo bench --bench pg_featureblob
```

### Python

Run from the repository root:

```bash
# install dependencies
cd python && uv sync && cd ..

# run a single benchmark
uv run --project python murr-bench redis_featureblob

# run with a custom config
uv run --project python murr-bench redis_feast --config configs/redis_feast.yaml

# save results to JSON
uv run --project python murr-bench pg_feast -o results/pg_feast.json

# available backends
uv run --project python murr-bench murr_http
uv run --project python murr-bench redis_feast
uv run --project python murr-bench redis_featureblob
uv run --project python murr-bench rocksdb
uv run --project python murr-bench pg_feast
uv run --project python murr-bench pg_featureblob
```

### Tests

```bash
# rust
cargo test

# python
cd python && uv run pytest tests/ -v
```

## Results: Rust time-to-last-byte benchmark

100M rows, 10 Float32 columns, 1000 random key lookups per iteration. All backends run on the same machine; container-backed ones use Docker via testcontainers. Memory is measured via Docker cgroup stats (container backends) or `/proc/self/statm` (embedded backends) as a before/after delta around the data load phase.

| Engine | Layout | Disk | Memory | Ingestion | p95 read latency |
|--------|--------|-----:|-------:|----------:|-----------------:|
| [murr](https://github.com/murrdb/murr) 0.1.8 | columnar | 4.8 GiB | 9.5 GiB | 2.76M rows/s | 443 us |
| Redis 8.6.1 | blob | 1.3 GiB | 10.6 GiB | 1.31M rows/s | 998 us |
| Redis 8.6.1 | HSET | 8.2 GiB | 21.2 GiB | 381K rows/s | 4.30 ms |
| RocksDB | blob | 4.3 GiB | 2.5 GiB | 2.40M rows/s | 3.85 ms |
| PostgreSQL 17 | blob | 12.8 GiB | 13.7 GiB | 283K rows/s | 9.75 ms |
| PostgreSQL 17 | col-per-feature | 12.7 GiB | 13.5 GiB | 138K rows/s | 8.79 ms |

**Notes on memory measurement:**
- Container backends (murr, Redis, PostgreSQL): memory delta is cgroup `usage`, which includes both anonymous (heap) and file-backed (mmap/page cache) pages.
- Embedded backends (RocksDB): memory delta is RSS from `/proc/self/statm`. This captures heap allocations but not OS page cache for SST files, so the number underestimates true memory footprint compared to container backends.

## Results: Python end-to-end benchmark

100M rows, 10 Float32 columns, 1000 random key lookups per iteration. Measures full round-trip latency including protocol decoding and `pd.DataFrame` conversion. Ingestion throughput includes Python-side serialization and batch writes.

| Engine | Layout | Ingestion | Read latency |
|--------|--------|----------:|-------------:|
| [murr](https://github.com/murrdb/murr) 0.1.8 | columnar | 2.34M rows/s | 1.38 ms |
| Redis 8.6.1 | blob | 136K rows/s | 2.42 ms |
| Redis 8.6.1 | HSET | 61K rows/s | 9.39 ms |
| RocksDB | blob | 622K rows/s | 4.90 ms |
| PostgreSQL 17 | blob | 356K rows/s | 10.8 ms |
| PostgreSQL 17 | col-per-feature | 143K rows/s | 10.6 ms |

## License

Apache-2.0 — see [LICENSE](LICENSE).
