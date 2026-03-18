from __future__ import annotations

from murr_bench.backend import Backend
from murr_bench.config import (
    MurrHttpConfig,
    RedisFeastConfig,
    RedisFeatureBlobConfig,
    RocksDbConfig,
    PgFeastConfig,
    PgFeatureBlobConfig,
)
from murr_bench.backends.murr_http import MurrHttp
from murr_bench.backends.redis import RedisFeast, RedisFeatureBlob
from murr_bench.backends.rocksdb import RocksDb
from murr_bench.backends.postgres import PgFeast, PgFeatureBlob

DEFAULT_CONFIGS: dict[str, str] = {
    "murr_http": "configs/murr_http.yaml",
    "redis_feast": "configs/redis_feast.yaml",
    "redis_featureblob": "configs/redis_featureblob.yaml",
    "rocksdb": "configs/rocksdb.yaml",
    "pg_feast": "configs/pg_feast.yaml",
    "pg_featureblob": "configs/pg_featureblob.yaml",
}


def get_backend(name: str, config: str | None = None) -> Backend:
    config_path = config or DEFAULT_CONFIGS.get(name)
    if config_path is None:
        raise ValueError(f"Unknown backend: {name}")

    match name:
        case "murr_http":
            return MurrHttp(MurrHttpConfig.from_file(config_path))
        case "redis_feast":
            return RedisFeast(RedisFeastConfig.from_file(config_path))
        case "redis_featureblob":
            return RedisFeatureBlob(RedisFeatureBlobConfig.from_file(config_path))
        case "rocksdb":
            return RocksDb(RocksDbConfig.from_file(config_path))
        case "pg_feast":
            return PgFeast(PgFeastConfig.from_file(config_path))
        case "pg_featureblob":
            return PgFeatureBlob(PgFeatureBlobConfig.from_file(config_path))
        case _:
            raise ValueError(f"Unknown backend: {name}")
