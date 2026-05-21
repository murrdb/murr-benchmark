from __future__ import annotations

from pathlib import Path
from typing import Literal, TypeVar

import yaml
from pydantic import BaseModel


class BenchConfig(BaseModel):
    total_rows: int
    select_rows: int
    select_cols: int
    write_batch_size: int
    measurement_time_secs: int
    warmup_time_secs: int
    sample_size: int


T = TypeVar("T", bound=BenchConfig)


def load_variants(cls: type[T], path: str | Path) -> list[tuple[str, T]]:
    """Load a multi-variant YAML and return one validated config per backend variant.

    The YAML's `backend:` key must be a map of named variants. Each variant becomes
    a separate config object with `backend` set to that variant's settings, so the
    rest of the harness can keep treating `config.backend` as a scalar.
    """
    with open(path) as f:
        raw = yaml.safe_load(f)
    backends = raw.pop("backend", None)
    if not isinstance(backends, dict):
        raise ValueError(
            f"{path}: `backend` must be a map of named variants, "
            f"e.g. `backend: {{ default: {{...}} }}`"
        )
    return [
        (name, cls.model_validate({**raw, "backend": b}))
        for name, b in backends.items()
    ]


class MurrHttpConfig(BenchConfig):
    class Backend(BaseModel):
        image: str
        cgroup_memory_mb: int | None = None

    backend: Backend


class RedisFeastConfig(BenchConfig):
    class Backend(BaseModel):
        image: str
        read_mode: Literal["hgetall", "hmget"]
        cgroup_memory_mb: int | None = None

    backend: Backend


class RedisFeatureBlobConfig(BenchConfig):
    class Backend(BaseModel):
        image: str
        cgroup_memory_mb: int | None = None

    backend: Backend


class RocksDbConfig(BenchConfig):
    class Backend(BaseModel):
        data_dir: Path
        table_format: Literal["block_based", "plain"] = "block_based"

        # PlainTable options (rocksdict.PlainTableFactoryOptions). Defaults mirror
        # src/backends/rocksdb.rs `default_*` helpers.
        plain_bloom_bits_per_key: int = 10
        plain_hash_table_ratio: float = 0.75
        plain_index_sparseness: int = 16
        plain_store_index_in_file: bool = False

        # BlockBased options (rocksdict.BlockBasedOptions). None for the bloom filter
        # means disabled, matching the Rust Option<f64>.
        bloom_filter_bits_per_key: float | None = None
        whole_key_filtering: bool = True
        block_size: int = 512
        block_cache_mb: int = 0
        cache_index_and_filter_blocks: bool = False
        pin_l0_filter_and_index_blocks: bool = False
        block_restart_interval: int = 8
        data_block_hash_index: bool = True
        data_block_hash_ratio: float = 0.75

        # Common Options.
        mmap_reads: bool = True
        use_direct_reads: bool = False
        async_io: bool = True
        verify_checksums: bool = False

        # Read-path. rocksdict only exposes a single Rdict.get(list) multi_get path,
        # so these are accepted for YAML compatibility with the Rust config but have
        # no effect on the Python harness.
        batched_multi_get: bool = True
        sorted_input: bool = True

    backend: Backend


class PgFeastConfig(BenchConfig):
    class Backend(BaseModel):
        image: str
        cgroup_memory_mb: int | None = None

    backend: Backend


class PgFeatureBlobConfig(BenchConfig):
    class Backend(BaseModel):
        image: str
        cgroup_memory_mb: int | None = None

    backend: Backend
