# murr-benchmark

Benchmarks for [murr](https://github.com/murrdb/murr) — a RocksDB-based NVMe/S3 cache for AI/ML inference workloads.

## Results: Rust time-to-last-byte benchmark

100M rows, 10 Float32 columns, 1000 random key lookups per iteration. Disk is reported for backends that persist to disk; Redis/Valkey/Dragonfly are pure in-memory. Memory is the container `TOTAL` (RSS+SHR) delta around the load phase. Net TX is server-to-client bytes per read. `disk` variants are cgroup-capped at 2 GiB RAM to force disk reads.

### Blob layouts

| Engine | Layout | Memory | Disk | Ingestion | p50 latency | Net TX/read |
|--------|--------|-------:|-----:|----------:|------------:|------------:|
| murr 0.2.0 mmap | native | 7.5 GiB | 5.9 GiB | 948K rows/s | 268 µs | 42 KiB |
| Dragonfly 1.31 | blob | 7.3 GiB | — | 4.01M rows/s | 296 µs | 46 KiB |
| Valkey 8.1 | blob | 8.9 GiB | — | 1.58M rows/s | 657 µs | 46 KiB |
| Redis 8.6.3 | blob | 9.6 GiB | — | 1.43M rows/s | 815 µs | 46 KiB |
| pgsql 18.4 | blob | 24.0 GiB | 12.8 GiB | 400K rows/s | 5.69 ms | 62 KiB |

### Hash / col-per-feature layouts

| Engine | Layout | Memory | Disk | Ingestion | p50 latency | Net TX/read |
|--------|--------|-------:|-----:|----------:|------------:|------------:|
| murr 0.2.0 mmap | native | 7.5 GiB | 5.9 GiB | 948K rows/s | 268 µs | 42 KiB |
| Dragonfly 1.31 | hash | 20.1 GiB | — | 650K rows/s | 2.82 ms | 213 KiB |
| Valkey 8.1 | hash | 19.4 GiB | — | 378K rows/s | 3.20 ms | 210 KiB |
| Redis 8.6.3 | hash | 20.1 GiB | — | 398K rows/s | 3.25 ms | 210 KiB |
| pgsql 18.4 | col | 23.4 GiB | 12.7 GiB | 384K rows/s | 6.54 ms | 86 KiB |

### Disk mode (2 GiB RAM cap)

| Engine | Layout | Memory | Disk | Ingestion | p50 latency | Net TX/read |
|--------|--------|-------:|-----:|----------:|------------:|------------:|
| murr 0.2.0 block | native | 1.7 GiB | 5.8 GiB | 1.00M rows/s | 6.33 ms | 42 KiB |
| pgsql 18.4 | blob | 2.0 GiB | 12.8 GiB | 329K rows/s | 189 ms | 62 KiB |
| pgsql 18.4 | col | 2.0 GiB | 12.7 GiB | 327K rows/s | 217 ms | 86 KiB |

## Benchmark methodology

The suite includes two complementary harnesses measuring different aspects of read performance:

**Rust (Criterion)** — measures raw service throughput as time-to-last-byte. The benchmark reads `select_rows` random keys from the backend per iteration and consumes the raw response bytes without decoding. This isolates the storage/network layer and shows the theoretical ceiling of each backend.

**Python (pyperf)** — measures end-to-end latency as experienced by a Python ML client. The benchmark performs the same random-key reads but includes full protocol decoding and conversion into a `pd.DataFrame`. This captures the real cost a user pays: HTTP/Redis/SQL protocol parsing, byte deserialization, and DataFrame construction.

Both harnesses share the same YAML config files and test data generation logic (random float32 columns with string keys), so results are directly comparable.

## Backends

| Backend | Transport | Container | Description |
|---------|-----------|-----------|-------------|
| `murr_http` | HTTP + Arrow IPC | `ghcr.io/murrdb/murr` | Murr server over HTTP (mmap and block storage modes) |
| `murr_embed` | In-process | — | Murr embedded library (Rust only) |
| `redis_feast` | RESP | `redis` | Redis with Feast-style HSET layout |
| `redis_featureblob` | RESP | `redis` | Redis with packed byte-blob layout |
| `valkey_feast` | RESP | `valkey/valkey` | Valkey with Feast-style HSET layout |
| `valkey_featureblob` | RESP | `valkey/valkey` | Valkey with packed byte-blob layout |
| `dragonfly_feast` | RESP | `dragonflydb/dragonfly` | Dragonfly with Feast-style HSET layout |
| `dragonfly_featureblob` | RESP | `dragonflydb/dragonfly` | Dragonfly with packed byte-blob layout |
| `rocksdb` | In-process | — | Local RocksDB key-value store |
| `pg_feast` | PostgreSQL | `postgres` | PostgreSQL with explicit typed columns |
| `pg_featureblob` | PostgreSQL | `postgres` | PostgreSQL with BYTEA blob column |

All container-backed backends use `testcontainers` to manage Docker lifecycle automatically.

## Data layouts

### Feast (hash-per-row)

Used by `redis_feast`, `valkey_feast`, `dragonfly_feast`, and `pg_feast`. Each entity key maps to a set of individually named feature columns. In Redis-family backends this is an HSET with one field per feature; in PostgreSQL it is a table with explicit `REAL` columns.

```
key="42" -> { col_0: 0.71, col_1: 0.33, col_2: 0.89, ... }
```

This layout mirrors [Feast](https://feast.dev/) online store format. It allows reading a subset of columns but has per-field overhead.

### Feature blob (packed binary)

Used by `redis_featureblob`, `valkey_featureblob`, `dragonfly_featureblob`, `pg_featureblob`, and `rocksdb`. All feature values for an entity are concatenated into a single byte buffer of little-endian float32 values.

```
key="42" -> b"\xcd\xcc\x34\x3f\xa4\x70\xa8\x3e..."  (N × 4 bytes)
```

Compact and cache-friendly — a single read returns all features. The client unpacks with `np.frombuffer(blob, dtype='<f4')`. No per-column overhead, but always reads all columns.

### Arrow IPC (columnar)

Used by `murr_http`. Data is exchanged as Apache Arrow IPC streams — a columnar binary wire format with zero-copy read support. Writes send `RecordBatch` via Arrow stream format; reads return the same. (Server-side storage is row-wise on top of RocksDB SSTables; columnar refers to the wire format.)

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
cargo bench --bench murr_http
cargo bench --bench murr_embed
cargo bench --bench redis_feast
cargo bench --bench redis_featureblob
cargo bench --bench valkey_feast
cargo bench --bench valkey_featureblob
cargo bench --bench dragonfly_feast
cargo bench --bench dragonfly_featureblob
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
uv run --project python murr-bench valkey_feast
uv run --project python murr-bench valkey_featureblob
uv run --project python murr-bench dragonfly_feast
uv run --project python murr-bench dragonfly_featureblob
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

## Results: Python end-to-end benchmark

100M rows, 10 Float32 columns, 1000 random key lookups per iteration. Measures full round-trip latency including protocol decoding and `pd.DataFrame` conversion. Ingestion throughput includes Python-side serialization and batch writes.

### Blob layouts

| Engine | Layout | Ingestion | Read latency |
|--------|--------|----------:|-------------:|
| murr 0.2.0 mmap | native | 1.06M rows/s | 1.08 ms |
| Dragonfly | blob | 524K rows/s | 1.68 ms |
| Valkey 8.1 | blob | 436K rows/s | 2.04 ms |
| Redis 8.6.3 | blob | 421K rows/s | 2.46 ms |
| pgsql 18.4 | blob | 298K rows/s | 28.6 ms |

### Hash / col-per-feature layouts

| Engine | Layout | Ingestion | Read latency |
|--------|--------|----------:|-------------:|
| murr 0.2.0 mmap | native | 1.06M rows/s | 1.08 ms |
| Dragonfly | hash | 64K rows/s | 8.25 ms |
| Valkey 8.1 | hash | 62K rows/s | 8.63 ms |
| Redis 8.6.3 | hash | 62K rows/s | 8.50 ms |
| pgsql 18.4 | col | 271K rows/s | 13.8 ms |

### Disk mode (2 GiB RAM cap)

| Engine | Layout | Ingestion | Read latency |
|--------|--------|----------:|-------------:|
| murr 0.2.0 block | native | 662K rows/s | 6.69 ms |
| pgsql 18.4 | blob | 317K rows/s | 171 ms |
| pgsql 18.4 | col | 303K rows/s | 153 ms |

## License

Apache-2.0 — see [LICENSE](LICENSE).
