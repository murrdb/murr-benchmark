from __future__ import annotations

from murr_bench.backend import Backend
from murr_bench.config import (
    BenchConfig,
    MurrHttpConfig,
    PgFeastConfig,
    PgFeatureBlobConfig,
    RedisFeastConfig,
    RedisFeatureBlobConfig,
    RocksDbConfig,
    load_variants,
)
from murr_bench.backends.murr_http import MurrHttp
from murr_bench.backends.postgres import PgFeast, PgFeatureBlob
from murr_bench.backends.redis import RedisFeast, RedisFeatureBlob
from murr_bench.backends.rocksdb import RocksDb

DEFAULT_CONFIGS: dict[str, str] = {
    "murr_http": "configs/murr_http.yaml",
    "redis_feast": "configs/redis_feast.yaml",
    "redis_featureblob": "configs/redis_featureblob.yaml",
    "rocksdb": "configs/rocksdb.yaml",
    "pg_feast": "configs/pg_feast.yaml",
    "pg_featureblob": "configs/pg_featureblob.yaml",
}

_CONFIG_CLASSES: dict[str, type[BenchConfig]] = {
    "murr_http": MurrHttpConfig,
    "redis_feast": RedisFeastConfig,
    "redis_featureblob": RedisFeatureBlobConfig,
    "rocksdb": RocksDbConfig,
    "pg_feast": PgFeastConfig,
    "pg_featureblob": PgFeatureBlobConfig,
}

_BACKEND_CLASSES: dict[str, type[Backend]] = {
    "murr_http": MurrHttp,
    "redis_feast": RedisFeast,
    "redis_featureblob": RedisFeatureBlob,
    "rocksdb": RocksDb,
    "pg_feast": PgFeast,
    "pg_featureblob": PgFeatureBlob,
}


def get_backends(name: str, config: str | None = None) -> list[tuple[str, Backend]]:
    """Load the YAML at `config` (or the default for `name`) and return one
    (variant_name, backend_instance) pair per backend variant in the file."""
    config_path = config or DEFAULT_CONFIGS.get(name)
    if config_path is None:
        raise ValueError(f"Unknown backend: {name}")

    cls = _CONFIG_CLASSES.get(name)
    ctor = _BACKEND_CLASSES.get(name)
    if cls is None or ctor is None:
        raise ValueError(f"Unknown backend: {name}")

    return [(variant, ctor(cfg)) for variant, cfg in load_variants(cls, config_path)]
