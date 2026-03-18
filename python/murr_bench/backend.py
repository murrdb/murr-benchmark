from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd
import pyarrow as pa

from murr_bench.config import BenchConfig


class Backend(ABC):
    config: BenchConfig

    @abstractmethod
    async def init(self) -> None: ...

    @abstractmethod
    async def write_batch(self, batch: pa.RecordBatch) -> None: ...

    @abstractmethod
    async def read(self, keys: list[str], columns: list[str]) -> pd.DataFrame: ...

    async def flush(self) -> None:
        pass

    @abstractmethod
    async def cleanup(self) -> None: ...
