from __future__ import annotations

import logging
import struct

import numpy as np
import pandas as pd
import pyarrow as pa
import redis.asyncio as aioredis
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from murr_bench.backend import Backend
from murr_bench.config import RedisFeastConfig, RedisFeatureBlobConfig

logger = logging.getLogger(__name__)

REDIS_PORT = 6379


class RedisFeast(Backend):
    def __init__(self, config: RedisFeastConfig) -> None:
        self.config = config
        self._container: DockerContainer | None = None
        self._redis: aioredis.Redis | None = None

    async def init(self) -> None:
        self._container = (
            DockerContainer(self.config.backend.image)
            .with_exposed_ports(REDIS_PORT)
        )
        self._container.waiting_for(LogMessageWaitStrategy("Ready to accept connections"))
        self._container.start()
        host = self._container.get_container_host_ip()
        port = self._container.get_exposed_port(REDIS_PORT)
        self._redis = aioredis.from_url(f"redis://{host}:{port}")
        logger.info("redis_feast: connected at %s:%s", host, port)

    async def write_batch(self, batch: pa.RecordBatch) -> None:
        assert self._redis is not None
        keys = batch.column("key").to_pylist()
        num_cols = batch.num_columns - 1
        col_names = [batch.schema.field(i + 1).name for i in range(num_cols)]
        values = np.column_stack(
            [batch.column(i + 1).to_numpy() for i in range(num_cols)]
        ).astype(np.float32)

        pipe = self._redis.pipeline(transaction=False)
        for row_idx, key in enumerate(keys):
            fields: dict[str, bytes] = {}
            for col_idx, col_name in enumerate(col_names):
                fields[col_name] = struct.pack("<f", float(values[row_idx, col_idx]))
            pipe.hset(key, mapping=fields)
        await pipe.execute()

    async def read(self, keys: list[str], columns: list[str]) -> pd.DataFrame:
        assert self._redis is not None
        pipe = self._redis.pipeline(transaction=False)

        if self.config.backend.read_mode == "hgetall":
            for key in keys:
                pipe.hgetall(key)
            results = await pipe.execute()
            rows = []
            for hash_data in results:
                row = [
                    struct.unpack("<f", hash_data[col.encode()])[0]
                    for col in columns
                ]
                rows.append(row)
        else:
            for key in keys:
                pipe.hmget(key, *columns)
            results = await pipe.execute()
            rows = []
            for vals in results:
                row = [struct.unpack("<f", v)[0] for v in vals]
                rows.append(row)

        return pd.DataFrame(rows, columns=columns)

    async def cleanup(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
        if self._container is not None:
            self._container.stop()


class RedisFeatureBlob(Backend):
    def __init__(self, config: RedisFeatureBlobConfig) -> None:
        self.config = config
        self._container: DockerContainer | None = None
        self._redis: aioredis.Redis | None = None

    async def init(self) -> None:
        self._container = (
            DockerContainer(self.config.backend.image)
            .with_exposed_ports(REDIS_PORT)
        )
        self._container.waiting_for(LogMessageWaitStrategy("Ready to accept connections"))
        self._container.start()
        host = self._container.get_container_host_ip()
        port = self._container.get_exposed_port(REDIS_PORT)
        self._redis = aioredis.from_url(f"redis://{host}:{port}")
        logger.info("redis_featureblob: connected at %s:%s", host, port)

    async def write_batch(self, batch: pa.RecordBatch) -> None:
        assert self._redis is not None
        keys = batch.column("key").to_pylist()
        num_cols = batch.num_columns - 1
        values = np.column_stack(
            [batch.column(i + 1).to_numpy() for i in range(num_cols)]
        ).astype(np.float32)

        pipe = self._redis.pipeline(transaction=False)
        for row_idx, key in enumerate(keys):
            pipe.set(key, values[row_idx].tobytes())
        await pipe.execute()

    async def read(self, keys: list[str], columns: list[str]) -> pd.DataFrame:
        assert self._redis is not None
        blobs: list[bytes | None] = await self._redis.mget(keys)
        rows = np.stack(
            [np.frombuffer(b, dtype="<f4") for b in blobs if b is not None]
        )
        return pd.DataFrame(rows, columns=columns)

    async def cleanup(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
        if self._container is not None:
            self._container.stop()
