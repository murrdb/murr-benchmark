from __future__ import annotations

import asyncio
import logging
import tempfile
from pathlib import Path

import httpx
import pandas as pd
import pyarrow as pa
import yaml
from murr import MurrClientAsync, TableSchema, ColumnSchema, DType
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from murr_bench.backend import Backend
from murr_bench.config import MurrHttpConfig
from murr_bench.testdata import column_names

logger = logging.getLogger(__name__)

MURR_PORT = 8080
CONTAINER_DATA_DIR = "/tmp/murr-bench"
CONTAINER_CONFIG_PATH = "/etc/murr/config.yaml"


class MurrHttp(Backend):
    def __init__(self, config: MurrHttpConfig) -> None:
        self.config = config
        self._container: DockerContainer | None = None
        self._client: MurrClientAsync | None = None
        self._config_file: Path | None = None

    async def init(self) -> None:
        # `model_extra` holds everything in the YAML's `backend:` block that
        # isn't `image` / `cgroup_memory_mb` — i.e. the `mmap:` or `block:` key
        # (with its options). Pass it through verbatim to the murr server.
        storage_extras = self.config.backend.model_extra or {}
        server_yaml = {
            "storage": {
                "path": CONTAINER_DATA_DIR,
                **storage_extras,
            }
        }
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        )
        yaml.safe_dump(server_yaml, tmp)
        tmp.close()
        self._config_file = Path(tmp.name)

        self._container = (
            DockerContainer(self.config.backend.image)
            .with_exposed_ports(MURR_PORT)
            .with_volume_mapping(
                str(self._config_file), CONTAINER_CONFIG_PATH, "ro"
            )
            .with_command(["--config", CONTAINER_CONFIG_PATH])
        )
        if self.config.backend.cgroup_memory_mb is not None:
            self._container = self._container.with_kwargs(
                mem_limit=f"{self.config.backend.cgroup_memory_mb}m"
            )
        self._container.waiting_for(LogMessageWaitStrategy("Starting murr"))
        self._container.start()
        await asyncio.sleep(1.0)

        host = self._container.get_container_host_ip()
        port = self._container.get_exposed_port(MURR_PORT)
        endpoint = f"http://{host}:{port}"

        self._client = MurrClientAsync(endpoint)
        self._client._client.timeout = httpx.Timeout(120.0)

        columns: dict[str, ColumnSchema] = {
            "key": ColumnSchema(dtype=DType.UTF8, nullable=False),
        }
        for name in column_names(self.config.select_cols):
            columns[name] = ColumnSchema(dtype=DType.FLOAT32, nullable=False)

        await self._client.create_table(
            "bench", TableSchema(key="key", columns=columns)
        )
        logger.info(
            "murr_http: table created at %s (storage=%s)",
            endpoint,
            ",".join(storage_extras.keys()) or "default",
        )

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
        if self._config_file is not None:
            self._config_file.unlink(missing_ok=True)
