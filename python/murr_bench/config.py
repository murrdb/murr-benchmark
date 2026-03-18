from __future__ import annotations

from pathlib import Path
from typing import Literal, Self

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

    @classmethod
    def from_file(cls, path: str | Path) -> Self:
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)


class MurrHttpConfig(BenchConfig):
    class Backend(BaseModel):
        image: str

    backend: Backend


class RedisFeastConfig(BenchConfig):
    class Backend(BaseModel):
        image: str
        read_mode: Literal["hgetall", "hmget"]

    backend: Backend


class RedisFeatureBlobConfig(BenchConfig):
    class Backend(BaseModel):
        image: str

    backend: Backend


class RocksDbConfig(BenchConfig):
    class Backend(BaseModel):
        data_dir: Path

    backend: Backend


class PgFeastConfig(BenchConfig):
    class Backend(BaseModel):
        image: str

    backend: Backend


class PgFeatureBlobConfig(BenchConfig):
    class Backend(BaseModel):
        image: str

    backend: Backend
