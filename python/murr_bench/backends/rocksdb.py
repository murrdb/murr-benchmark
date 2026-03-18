from __future__ import annotations

import logging
import shutil

import numpy as np
import pandas as pd
import pyarrow as pa
from rocksdict import Rdict

from murr_bench.backend import Backend
from murr_bench.config import RocksDbConfig

logger = logging.getLogger(__name__)


class RocksDb(Backend):
    def __init__(self, config: RocksDbConfig) -> None:
        self.config = config
        self._db: Rdict | None = None

    async def init(self) -> None:
        self._db = Rdict(str(self.config.backend.data_dir))
        logger.info("rocksdb: opened at %s", self.config.backend.data_dir)

    async def write_batch(self, batch: pa.RecordBatch) -> None:
        assert self._db is not None
        keys = batch.column("key").to_pylist()
        num_cols = batch.num_columns - 1
        values = np.column_stack(
            [batch.column(i + 1).to_numpy() for i in range(num_cols)]
        ).astype(np.float32)

        for row_idx, key in enumerate(keys):
            self._db[key] = values[row_idx].tobytes()

    async def read(self, keys: list[str], columns: list[str]) -> pd.DataFrame:
        assert self._db is not None
        blobs = [self._db[k] for k in keys]
        rows = np.stack(
            [np.frombuffer(b, dtype="<f4") for b in blobs]
        )
        return pd.DataFrame(rows, columns=columns)

    async def flush(self) -> None:
        assert self._db is not None
        self._db.flush()
        self._db.compact_range(None, None)

    async def cleanup(self) -> None:
        if self._db is not None:
            self._db.close()
        shutil.rmtree(str(self.config.backend.data_dir), ignore_errors=True)
