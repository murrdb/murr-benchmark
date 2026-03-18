from __future__ import annotations

import logging

import pandas as pd
import pyarrow as pa
from murr import MurrClientAsync, TableSchema, ColumnSchema, DType
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from murr_bench.backend import Backend
from murr_bench.config import MurrHttpConfig
from murr_bench.testdata import column_names

logger = logging.getLogger(__name__)

MURR_PORT = 8080


class MurrHttp(Backend):
    def __init__(self, config: MurrHttpConfig) -> None:
        self.config = config
        self._container: DockerContainer | None = None
        self._client: MurrClientAsync | None = None

    async def init(self) -> None:
        self._container = (
            DockerContainer(self.config.backend.image)
            .with_exposed_ports(MURR_PORT)
        )
        self._container.waiting_for(LogMessageWaitStrategy("Starting murr"))
        self._container.start()

        host = self._container.get_container_host_ip()
        port = self._container.get_exposed_port(MURR_PORT)
        endpoint = f"http://{host}:{port}"

        self._client = MurrClientAsync(endpoint)

        columns: dict[str, ColumnSchema] = {
            "key": ColumnSchema(dtype=DType.UTF8, nullable=False),
        }
        for name in column_names(self.config.select_cols):
            columns[name] = ColumnSchema(dtype=DType.FLOAT32, nullable=False)

        await self._client.create_table(
            "bench", TableSchema(key="key", columns=columns)
        )
        logger.info("murr_http: table created at %s", endpoint)

    async def write_batch(self, batch: pa.RecordBatch) -> None:
        assert self._client is not None
        await self._client.write("bench", batch)

    async def read(self, keys: list[str], columns: list[str]) -> pd.DataFrame:
        assert self._client is not None
        rb = await self._client.read("bench", keys, columns)
        return rb.to_pandas()

    async def cleanup(self) -> None:
        if self._client is not None:
            await self._client.close()
        if self._container is not None:
            self._container.stop()
