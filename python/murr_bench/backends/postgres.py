from __future__ import annotations

import asyncio
import logging

import numpy as np
import pandas as pd
import psycopg
import pyarrow as pa
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from murr_bench.backend import Backend
from murr_bench.config import PgFeastConfig, PgFeatureBlobConfig

logger = logging.getLogger(__name__)

PG_PORT = 5432


async def _start_pg(image: str) -> tuple[DockerContainer, psycopg.AsyncConnection]:
    container = (
        DockerContainer(image)
        .with_exposed_ports(PG_PORT)
        .with_env("POSTGRES_PASSWORD", "bench")
        .with_env("POSTGRES_DB", "bench")
    )
    container.start()
    wait_for_logs(container, "database system is ready to accept connections")

    host = container.get_container_host_ip()
    port = container.get_exposed_port(PG_PORT)
    conninfo = f"host={host} port={port} user=postgres password=bench dbname=bench"

    conn: psycopg.AsyncConnection | None = None
    for _ in range(50):
        try:
            conn = await psycopg.AsyncConnection.connect(conninfo, autocommit=True)
            break
        except psycopg.OperationalError:
            await asyncio.sleep(0.2)

    assert conn is not None, "failed to connect to Postgres"
    logger.info("postgres: connected at %s:%s", host, port)
    return container, conn


class PgFeast(Backend):
    def __init__(self, config: PgFeastConfig) -> None:
        self.config = config
        self._container: DockerContainer | None = None
        self._conn: psycopg.AsyncConnection | None = None

    async def init(self) -> None:
        self._container, self._conn = await _start_pg(self.config.backend.image)

        col_defs = ", ".join(
            f"col_{i} REAL NOT NULL" for i in range(self.config.select_cols)
        )
        await self._conn.execute(
            f"CREATE TABLE bench (key TEXT PRIMARY KEY, {col_defs})"
        )

    async def write_batch(self, batch: pa.RecordBatch) -> None:
        assert self._conn is not None
        keys = batch.column("key").to_pylist()
        num_cols = batch.num_columns - 1
        col_names = [batch.schema.field(i + 1).name for i in range(num_cols)]
        values = np.column_stack(
            [batch.column(i + 1).to_numpy() for i in range(num_cols)]
        ).astype(np.float32)

        cols = ["key"] + col_names
        async with self._conn.cursor() as cur:
            async with cur.copy(
                f"COPY bench ({', '.join(cols)}) FROM STDIN"
            ) as copy:
                for row_idx in range(len(keys)):
                    row_vals = [keys[row_idx]] + [
                        str(float(values[row_idx, col_idx]))
                        for col_idx in range(num_cols)
                    ]
                    await copy.write_row(row_vals)

    async def read(self, keys: list[str], columns: list[str]) -> pd.DataFrame:
        assert self._conn is not None
        col_list = ", ".join(columns)
        sql = f"SELECT {col_list} FROM bench WHERE key = ANY(%s)"
        async with self._conn.cursor() as cur:
            await cur.execute(sql, (keys,))
            rows = await cur.fetchall()
        return pd.DataFrame(rows, columns=columns)

    async def flush(self) -> None:
        assert self._conn is not None
        await self._conn.execute("CHECKPOINT")

    async def cleanup(self) -> None:
        if self._conn is not None:
            await self._conn.close()
        if self._container is not None:
            self._container.stop()


class PgFeatureBlob(Backend):
    def __init__(self, config: PgFeatureBlobConfig) -> None:
        self.config = config
        self._container: DockerContainer | None = None
        self._conn: psycopg.AsyncConnection | None = None

    async def init(self) -> None:
        self._container, self._conn = await _start_pg(self.config.backend.image)

        await self._conn.execute(
            "CREATE TABLE bench (key TEXT PRIMARY KEY, value BYTEA NOT NULL)"
        )

    async def write_batch(self, batch: pa.RecordBatch) -> None:
        assert self._conn is not None
        keys = batch.column("key").to_pylist()
        num_cols = batch.num_columns - 1
        values = np.column_stack(
            [batch.column(i + 1).to_numpy() for i in range(num_cols)]
        ).astype(np.float32)

        async with self._conn.cursor() as cur:
            async with cur.copy(
                "COPY bench (key, value) FROM STDIN WITH BINARY"
            ) as copy:
                for row_idx in range(len(keys)):
                    await copy.write_row((keys[row_idx], values[row_idx].tobytes()))

    async def read(self, keys: list[str], columns: list[str]) -> pd.DataFrame:
        assert self._conn is not None
        sql = "SELECT key, value FROM bench WHERE key = ANY(%s)"
        async with self._conn.cursor() as cur:
            await cur.execute(sql, (keys,))
            rows = await cur.fetchall()

        data = np.stack(
            [np.frombuffer(row[1], dtype="<f4") for row in rows]
        )
        return pd.DataFrame(data, columns=columns)

    async def flush(self) -> None:
        assert self._conn is not None
        await self._conn.execute("CHECKPOINT")

    async def cleanup(self) -> None:
        if self._conn is not None:
            await self._conn.close()
        if self._container is not None:
            self._container.stop()
