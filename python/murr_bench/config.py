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
