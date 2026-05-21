import tempfile

import pytest

from murr_bench.backends.rocksdb import RocksDb
from murr_bench.config import RocksDbConfig
from tests.conftest import *


def _make_config(tmp: str, **backend_kwargs) -> RocksDbConfig:
    return RocksDbConfig(
        total_rows=TOTAL_ROWS,
        select_rows=SELECT_ROWS,
        select_cols=SELECT_COLS,
        write_batch_size=WRITE_BATCH_SIZE,
        measurement_time_secs=MEASUREMENT_TIME_SECS,
        warmup_time_secs=WARMUP_TIME_SECS,
        sample_size=SAMPLE_SIZE,
        backend=RocksDbConfig.Backend(data_dir=tmp, **backend_kwargs),
    )


async def test_roundtrip_block_based():
    with tempfile.TemporaryDirectory() as tmp:
        await roundtrip(RocksDb(_make_config(tmp, table_format="block_based")))


async def test_roundtrip_plain_table():
    with tempfile.TemporaryDirectory() as tmp:
        await roundtrip(RocksDb(_make_config(tmp, table_format="plain")))


async def test_rejects_nonempty_data_dir():
    with tempfile.TemporaryDirectory() as tmp:
        # pre-populate the dir so the empty-dir guard trips
        with open(f"{tmp}/sentinel", "w") as f:
            f.write("not empty")
        backend = RocksDb(_make_config(tmp))
        with pytest.raises(RuntimeError, match="not empty"):
            await backend.init()
